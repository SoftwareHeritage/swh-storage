.. _swh-storage-masking:

Object Masking
==============

For legal and policy reasons, some objects stored in the archive can be made
unavailable to the public. This restriction is implemented by the :mod:`masking
storage proxy <swh.storage.proxies.masking>`.

Data model
----------

This proxy has its own PostgreSQL database of:

* masking requests, identified by a private human-readable identifier (named the
  request's "slug"), and a public opaque UUID,
* for each request, a free-form message explaining the reason for the request,
  and a list of timestamped human-readable free-form history messages
* for each request, a list of affected "extended SWHIDs" (ie. :ref:`SWHIDs
  <persistent-identifiers>` with extra types ``ori`` for origins and ``emd`` for
  Raw Extrinsic Metadata),
  each associated with a :class:`state <swh.storage.proxies.masking.db.MaskedState>`
  among ``VISIBLE``, ``PENDING_DECISION``, or ``RESTRICTED``

Requests are initially created with an empty set of SWHIDs, and SWHIDs are then
upserted with their evolving states.
Typically, all SWHIDs affected by a request will be added with state
``PENDING_DECISION`` shortly after a request is created, then each SWHID will be
moved to either ``VISIBLE`` or ``RESTRICTED`` as the request is being processed.

If a request is withdrawn or rejected, the set of associated SWHIDs will be
emptied, and the history messages kept.

Implementation
--------------

When any method of the masking proxy is called, the proxy will first forward the
call to its backend storage. For each object returned by the backend, the proxy
then checks whether that object should be masked. In particular, this means that
if an object is absent from the archive, then masking its own SWHID has no
effect; The masking will still affect the ExtIDs and Raw Extrinsic Metadata
objects targeting the masked object.

Objects are masked under any of these circumstances:

* They intrinsically have a SWHID, and that SWHID has at least one
  non-``VISIBLE`` status in the masking database
* The object is an origin, origin visit, or origin visit status, whose origin URL
  is masked (via the extended SWHID ``swh:1:ori:`` followed by the sha1 hash of the URL)
* The object is a Raw Extrinsic Metadata whose git-like SHA1 preceded by ``swh:1:emd:``
  is an extended SWHID with non-``VISIBLE`` status
* The object is an ExtID or Raw Extrinsic Metadata, and its ``target`` is a masked object,
  even if the target object is missing from the archive

To help the common storage unit tests which happen on a very small dataset
succeed, methods returning a random object from storage re-try the backend call
up to 5 times when the backend call returns a masked object, and fall back to
returning :const:`None`. Retries should be very scarce under real-world
conditions.

With the notable exception of ExtIDs and Raw Extrinsic Metadata, which we
consider to be extensions of their target objects, the objects that reference a
masked object are not masked themselves, and their attributes will still
reference the SWHID of the referenced masked objects undisturbed. This makes the
impact of masking an object as limited as possible, so that the feature can be
applied liberally.

If the masking proxy notices that any object that would be returned is masked, it fails
the whole method call, and returns a :exc:`swh.storage.exc.MaskedObjectException`.
This exception has a ``masked`` attribute, which is a dictionary mapping the
extended SWHIDs of every masked object that would have been returned to a list
of all the masking request UUIDs and associated state that affect them.
This allows clients to either display the exception to their user, or filter-out
masked SWHIDs from the list of objects before calling the method again.
