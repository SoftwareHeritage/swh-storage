# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .directory import DirectoryCooker
from .revision_flat import RevisionFlatCooker

COOKER_TYPES = {
    'directory': DirectoryCooker,
    'revision_flat': RevisionFlatCooker,
}
