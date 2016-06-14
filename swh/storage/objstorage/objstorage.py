# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


ID_HASH_ALGO = 'sha1'
ID_HASH_LENGTH = 40  # Size in bytes of the hash hexadecimal representation.


class ObjStorage():
    """ High-level API to manipulate the Software Heritage object storage.

    Conceptually, the object storage offers 5 methods:

    - __contains__()  check if an object is present, by object id
    - add()           add a new object, returning an object id
    - restore()       same as add() but erase an already existed content
    - get()           retrieve the content of an object, by object id
    - check()         check the integrity of an object, by object id

    And some management methods:

    - get_random()    get random object id of existing contents (used for the
                      content integrity checker).

    Each implementation of this interface can have a different behavior and
    its own way to store the contents.
    """

    def __contains__(self, *args, **kwargs):
        raise NotImplementedError(
            "Implementations of ObjStorage must have a '__contains__' method"
        )

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        """ Add a new object to the object storage.

        Args:
            content: content of the object to be added to the storage.
            obj_id: checksum of [bytes] using [ID_HASH_ALGO] algorithm. When
                given, obj_id will be trusted to match the bytes. If missing,
                obj_id will be computed on the fly.
            check_presence: indicate if the presence of the content should be
                verified before adding the file.

        Returns:
            the id of the object into the storage.
        """
        raise NotImplementedError(
            "Implementations of ObjStorage must have a 'add' method"
        )

    def restore(self, content, obj_id=None, *args, **kwargs):
        """ Restore a content that have been corrupted.

        This function is identical to add_bytes but does not check if
        the object id is already in the file system.

        Args:
            content: content of the object to be added to the storage
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.
        """
        raise NotImplemented(
            "Implementations of ObjStorage must have a 'restore' method"
        )

    def get(self, obj_id, *args, **kwargs):
        """ Retrieve the content of a given object.

        Args:
            obj_id: object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.
        """
        raise NotImplementedError(
            "Implementations of ObjStorage must have a 'get' method"
        )

    def check(self, obj_id, *args, **kwargs):
        """ Perform an integrity check for a given object.

        Verify that the file object is in place and that the gziped content
        matches the object id.

        Args:
            obj_id: object id.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.
        """
        raise NotImplementedError(
            "Implementations of ObjStorage must have a 'check' method"
        )

    def get_random(self, batch_size, *args, **kwargs):
        """ Get random ids of existing contents

        This method is used in order to get random ids to perform
        content integrity verifications on random contents.

        Attributes:
            batch_size (int): Number of ids that will be given

        Yields:
            An iterable of ids of contents that are in the current object
            storage.
        """
        raise NotImplementedError(
            "The current implementation of ObjStorage does not support "
            "'get_random' operation"
        )
