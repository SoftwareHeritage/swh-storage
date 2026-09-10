# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from enum import Enum
import functools
import inspect
from typing import Callable, Dict, Optional, Sequence, TypeVar, Union

from swh.core.statsd import statsd
from swh.model.model import BaseModel
from swh.model.swhids import ExtendedSWHID
from swh.storage import StorageSpec, get_storage
from swh.storage.interface import HashDict, ObjectDeletionInterface, StorageInterface
from swh.storage.metrics import COUNTED_ARG_METRIC, timed

_COUNTED_PARAMETER: Dict[str, Optional[str]] = {
    "content_get_data": None,
}
"""Maps each method to the name of one of its parameters, whose length should be measured

When absent, this defaults to the first parameter of sequence type (eg. list)."""


T = TypeVar("T", bound=Callable)


def _count_parameter(f: T, signature, counted_parameter: str, increment: Callable) -> T:
    """Calls ``increment`` with the length of the ``counted_parameter`` of ``f`` every time
    ``f`` is called.

    ``increment`` should have the same signature as :meth:`statsd.increment`.
    """
    tags = {"endpoint": f.__name__}

    @functools.wraps(f)
    def newf(*args, **kwargs):
        if counted_parameter is None:
            count = 1
        else:
            binding = signature.bind(*args, **kwargs)
            value = binding.arguments[counted_parameter]
            if value is None:
                count = 1
            else:
                count = len(value)
        increment(COUNTED_ARG_METRIC, count, tags=tags)

        # don't use the binding in case the underlying implementation supports more parameter
        # than StorageInterface
        return f(*args, **kwargs)

    return newf  # type: ignore[return-value]


def _get_counted_parameter(
    meth_name: str, signature: inspect.Signature
) -> Optional[str]:
    """Given a function signature, returns the name of the parameter that should be counted in metrics

    >>> from typing import *
    >>> import inspect
    >>> from swh.model.model import *
    >>> from swh.storage.interface import *

    >>> def revision_add(revisions: List[Revision]) -> Dict[str, int]: ...
    >>> _get_counted_parameter("revision_add", inspect.signature(revision_add))
    'revisions'

    >>> def revision_get(revision_ids: List[Sha1Git], ignore_displayname: bool = False) -> List[Optional[Revision]]: ...
    >>> _get_counted_parameter("revision_get", inspect.signature(revision_get))
    'revision_ids'

    >>> def revision_get_partition(partition_id: int, nb_partitions: int, page_token: Optional[str] = None, limit: int = 1000) -> PagedResult[Revision]: ...
    >>> _get_counted_parameter("revision_get_partition", inspect.signature(revision_get_partition)) is None
    True

    >>> def f(arg1: Optional[str], arg2: Optional[List[str]]): ...
    >>> _get_counted_parameter("f", inspect.signature(f)) is None
    True
    """  # noqa
    counted_parameters = []
    for parameter_name, parameter in signature.parameters.items():
        if parameter.default is not inspect.Signature.empty:
            # if the argument has a default value, then it's not the "main" argument
            # of the function.
            break

        type_ = parameter.annotation

        while hasattr(type_, "__origin__"):
            if type_.__origin__ is Union:
                if type(None) in type_.__args__:
                    # if the argument is optional, then it's not the "main" argument
                    # of the function.
                    break
            else:
                # dereference 'List[T]' to 'list'
                type_ = type_.__origin__

        if hasattr(type_, "__origin__"):
            assert type_.__origin__ is Union
            continue

        if type_ in (bool, bytes, int, str, ExtendedSWHID, datetime.datetime, HashDict):
            pass
        elif isinstance(type_, type) and issubclass(type_, (Enum, BaseModel)):
            pass
        elif isinstance(type_, type) and issubclass(type_, Sequence):
            counted_parameters.append(parameter_name)
        else:
            raise TypeError(
                f"Method {meth_name} is not configured in _COUNTING_PARAMETER "
                f"and one of its parameters is not a known type ({type_!r})."
            )

    if len(counted_parameters) == 0:
        return None
    elif len(counted_parameters) == 1:
        return counted_parameters[0]
    else:
        raise TypeError(
            f"Cannot decide which parameter should be counted for {meth_name} ({signature!r}). "
            f"Candidates: {counted_parameters!r}"
        )


class StatsdProxyStorage:
    """Storage implementation which times and counts calls to each endpoint,
    as well as the number of items in its list arguments (eg. the number of revision ids
    as parameter to ``revision_get`` or the number of revisions as parameter to
    ``revision_add``) and sends them to statsd

    Configuration:

    - ``storage``: configuration or instance of the storage being timed

    """

    def __init__(self, storage: Union[StorageSpec, StorageInterface]):
        self.storage: StorageInterface = (
            get_storage(**storage) if isinstance(storage, dict) else storage
        )

        for interface in (StorageInterface, ObjectDeletionInterface):
            for attribute_name in dir(interface):
                if attribute_name.startswith("_"):
                    continue
                attribute = getattr(self.storage, attribute_name)
                if hasattr(attribute, "__call__"):
                    signature = inspect.signature(attribute)
                    if attribute_name in _COUNTED_PARAMETER:
                        counted_parameter = _COUNTED_PARAMETER[attribute_name]
                    else:
                        counted_parameter = _get_counted_parameter(
                            attribute_name, signature
                        )

                    attribute = self._timed(attribute)
                    if counted_parameter is not None:
                        attribute = _count_parameter(
                            attribute,
                            signature,
                            counted_parameter,
                            increment=self._increment,
                        )

                    setattr(self, attribute_name, attribute)

    # overridden by the CountingProxyStorage to count in-memory instead of sending to statsd:

    def _timed(self, f: T) -> T:
        return timed(f)

    def _increment(self, metric: str, value: int, tags: dict[str, str]) -> None:
        statsd.increment(metric, value, tags=tags)
