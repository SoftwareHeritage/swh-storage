from .director import ArchiverWithRetentionPolicyDirector  # NOQA
from .director import ArchiverStdinToBackendDirector       # NOQA
from .worker import ArchiverWithRetentionPolicyWorker      # NOQA
from .worker import ArchiverToBackendWorker                # NOQA
from .copier import ArchiverCopier                         # NOQA

from swh.objstorage import register_objstorages
from swh.objstorage.cloud import AzureCloudObjStorage  # noqa

# Register new objstorage abilities
register_objstorages({
    'azure-storage': AzureCloudObjStorage
})
