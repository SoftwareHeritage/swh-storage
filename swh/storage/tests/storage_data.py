# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from swh.model.hashutil import hash_to_bytes
from swh.model import from_disk


class StorageData:
    def __getattr__(self, key):
        try:
            v = globals()[key]
        except KeyError as e:
            raise AttributeError(e.args[0])
        if hasattr(v, "copy"):
            return v.copy()
        return v


data = StorageData()


cont = {
    "data": b"42\n",
    "length": 3,
    "sha1": hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689"),
    "sha1_git": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    "sha256": hash_to_bytes(
        "673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a"
    ),
    "blake2s256": hash_to_bytes(
        "d5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d"
    ),
    "status": "visible",
}

cont2 = {
    "data": b"4242\n",
    "length": 5,
    "sha1": hash_to_bytes("61c2b3a30496d329e21af70dd2d7e097046d07b7"),
    "sha1_git": hash_to_bytes("36fade77193cb6d2bd826161a0979d64c28ab4fa"),
    "sha256": hash_to_bytes(
        "859f0b154fdb2d630f45e1ecae4a862915435e663248bb8461d914696fc047cd"
    ),
    "blake2s256": hash_to_bytes(
        "849c20fad132b7c2d62c15de310adfe87be94a379941bed295e8141c6219810d"
    ),
    "status": "visible",
}

cont3 = {
    "data": b"424242\n",
    "length": 7,
    "sha1": hash_to_bytes("3e21cc4942a4234c9e5edd8a9cacd1670fe59f13"),
    "sha1_git": hash_to_bytes("c932c7649c6dfa4b82327d121215116909eb3bea"),
    "sha256": hash_to_bytes(
        "92fb72daf8c6818288a35137b72155f507e5de8d892712ab96277aaed8cf8a36"
    ),
    "blake2s256": hash_to_bytes(
        "76d0346f44e5a27f6bafdd9c2befd304aff83780f93121d801ab6a1d4769db11"
    ),
    "status": "visible",
    "ctime": "2019-12-01 00:00:00Z",
}

contents = (cont, cont2, cont3)


missing_cont = {
    "data": b"missing\n",
    "length": 8,
    "sha1": hash_to_bytes("f9c24e2abb82063a3ba2c44efd2d3c797f28ac90"),
    "sha1_git": hash_to_bytes("33e45d56f88993aae6a0198013efa80716fd8919"),
    "sha256": hash_to_bytes(
        "6bbd052ab054ef222c1c87be60cd191addedd24cc882d1f5f7f7be61dc61bb3a"
    ),
    "blake2s256": hash_to_bytes(
        "306856b8fd879edb7b6f1aeaaf8db9bbecc993cd7f776c333ac3a782fa5c6eba"
    ),
    "status": "absent",
}

skipped_cont = {
    "length": 1024 * 1024 * 200,
    "sha1_git": hash_to_bytes("33e45d56f88993aae6a0198013efa80716fd8920"),
    "sha1": hash_to_bytes("43e45d56f88993aae6a0198013efa80716fd8920"),
    "sha256": hash_to_bytes(
        "7bbd052ab054ef222c1c87be60cd191addedd24cc882d1f5f7f7be61dc61bb3a"
    ),
    "blake2s256": hash_to_bytes(
        "ade18b1adecb33f891ca36664da676e12c772cc193778aac9a137b8dc5834b9b"
    ),
    "reason": "Content too long",
    "status": "absent",
    "origin": "file:///dev/zero",
}

skipped_cont2 = {
    "length": 1024 * 1024 * 300,
    "sha1_git": hash_to_bytes("44e45d56f88993aae6a0198013efa80716fd8921"),
    "sha1": hash_to_bytes("54e45d56f88993aae6a0198013efa80716fd8920"),
    "sha256": hash_to_bytes(
        "8cbd052ab054ef222c1c87be60cd191addedd24cc882d1f5f7f7be61dc61bb3a"
    ),
    "blake2s256": hash_to_bytes(
        "9ce18b1adecb33f891ca36664da676e12c772cc193778aac9a137b8dc5834b9b"
    ),
    "reason": "Content too long",
    "status": "absent",
}

skipped_contents = (skipped_cont, skipped_cont2)


dir = {
    "id": hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),
    "entries": (
        {
            "name": b"foo",
            "type": "file",
            "target": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),  # cont
            "perms": from_disk.DentryPerms.content,
        },
        {
            "name": b"bar\xc3",
            "type": "dir",
            "target": b"12345678901234567890",
            "perms": from_disk.DentryPerms.directory,
        },
    ),
}

dir2 = {
    "id": hash_to_bytes("8505808532953da7d2581741f01b29c04b1cb9ab"),
    "entries": (
        {
            "name": b"oof",
            "type": "file",
            "target": hash_to_bytes(  # cont2
                "36fade77193cb6d2bd826161a0979d64c28ab4fa"
            ),
            "perms": from_disk.DentryPerms.content,
        },
    ),
}

dir3 = {
    "id": hash_to_bytes("4ea8c6b2f54445e5dd1a9d5bb2afd875d66f3150"),
    "entries": (
        {
            "name": b"foo",
            "type": "file",
            "target": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),  # cont
            "perms": from_disk.DentryPerms.content,
        },
        {
            "name": b"subdir",
            "type": "dir",
            "target": hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),  # dir
            "perms": from_disk.DentryPerms.directory,
        },
        {
            "name": b"hello",
            "type": "file",
            "target": b"12345678901234567890",
            "perms": from_disk.DentryPerms.content,
        },
    ),
}

dir4 = {
    "id": hash_to_bytes("377aa5fcd944fbabf502dbfda55cd14d33c8c3c6"),
    "entries": (
        {
            "name": b"subdir1",
            "type": "dir",
            "target": hash_to_bytes("4ea8c6b2f54445e5dd1a9d5bb2afd875d66f3150"),  # dir3
            "perms": from_disk.DentryPerms.directory,
        },
    ),
}

directories = (dir, dir2, dir3, dir4)


minus_offset = datetime.timezone(datetime.timedelta(minutes=-120))
plus_offset = datetime.timezone(datetime.timedelta(minutes=120))

revision = {
    "id": hash_to_bytes("066b1b62dbfa033362092af468bf6cfabec230e7"),
    "message": b"hello",
    "author": {
        "name": b"Nicolas Dandrimont",
        "email": b"nicolas@example.com",
        "fullname": b"Nicolas Dandrimont <nicolas@example.com> ",
    },
    "date": {
        "timestamp": {"seconds": 1234567890, "microseconds": 0},
        "offset": 120,
        "negative_utc": False,
    },
    "committer": {
        "name": b"St\xc3fano Zacchiroli",
        "email": b"stefano@example.com",
        "fullname": b"St\xc3fano Zacchiroli <stefano@example.com>",
    },
    "committer_date": {
        "timestamp": {"seconds": 1123456789, "microseconds": 0},
        "offset": 0,
        "negative_utc": True,
    },
    "parents": (b"01234567890123456789", b"23434512345123456789"),
    "type": "git",
    "directory": hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),  # dir
    "metadata": {
        "checksums": {"sha1": "tarball-sha1", "sha256": "tarball-sha256",},
        "signed-off-by": "some-dude",
        "extra_headers": [
            ["gpgsig", b"test123"],
            ["mergetag", b"foo\\bar"],
            ["mergetag", b"\x22\xaf\x89\x80\x01\x00"],
        ],
    },
    "synthetic": True,
}

revision2 = {
    "id": hash_to_bytes("df7a6f6a99671fb7f7343641aff983a314ef6161"),
    "message": b"hello again",
    "author": {
        "name": b"Roberto Dicosmo",
        "email": b"roberto@example.com",
        "fullname": b"Roberto Dicosmo <roberto@example.com>",
    },
    "date": {
        "timestamp": {"seconds": 1234567843, "microseconds": 220000,},
        "offset": -720,
        "negative_utc": False,
    },
    "committer": {
        "name": b"tony",
        "email": b"ar@dumont.fr",
        "fullname": b"tony <ar@dumont.fr>",
    },
    "committer_date": {
        "timestamp": {"seconds": 1123456789, "microseconds": 0},
        "offset": 0,
        "negative_utc": False,
    },
    "parents": (b"01234567890123456789",),
    "type": "git",
    "directory": hash_to_bytes("8505808532953da7d2581741f01b29c04b1cb9ab"),  # dir2
    "metadata": None,
    "synthetic": False,
}

revision3 = {
    "id": hash_to_bytes("2cbd7bb22c653bbb23a29657852a50a01b591d46"),
    "message": b"a simple revision with no parents this time",
    "author": {
        "name": b"Roberto Dicosmo",
        "email": b"roberto@example.com",
        "fullname": b"Roberto Dicosmo <roberto@example.com>",
    },
    "date": {
        "timestamp": {"seconds": 1234567843, "microseconds": 220000,},
        "offset": -720,
        "negative_utc": False,
    },
    "committer": {
        "name": b"tony",
        "email": b"ar@dumont.fr",
        "fullname": b"tony <ar@dumont.fr>",
    },
    "committer_date": {
        "timestamp": {"seconds": 1127351742, "microseconds": 0},
        "offset": 0,
        "negative_utc": False,
    },
    "parents": (),
    "type": "git",
    "directory": hash_to_bytes("8505808532953da7d2581741f01b29c04b1cb9ab"),  # dir2
    "metadata": None,
    "synthetic": True,
}

revision4 = {
    "id": hash_to_bytes("88cd5126fc958ed70089d5340441a1c2477bcc20"),
    "message": b"parent of self.revision2",
    "author": {
        "name": b"me",
        "email": b"me@soft.heri",
        "fullname": b"me <me@soft.heri>",
    },
    "date": {
        "timestamp": {"seconds": 1244567843, "microseconds": 220000,},
        "offset": -720,
        "negative_utc": False,
    },
    "committer": {
        "name": b"committer-dude",
        "email": b"committer@dude.com",
        "fullname": b"committer-dude <committer@dude.com>",
    },
    "committer_date": {
        "timestamp": {"seconds": 1244567843, "microseconds": 220000,},
        "offset": -720,
        "negative_utc": False,
    },
    "parents": (
        hash_to_bytes("2cbd7bb22c653bbb23a29657852a50a01b591d46"),
    ),  # revision3
    "type": "git",
    "directory": hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),  # dir
    "metadata": None,
    "synthetic": False,
}

revisions = (revision, revision2, revision3, revision4)


origin = {
    "url": "file:///dev/null",
}

origin2 = {
    "url": "file:///dev/zero",
}

origins = (origin, origin2)


metadata_authority = {
    "type": "deposit",
    "url": "http://hal.inria.example.com/",
    "metadata": {"location": "France"},
}
metadata_authority2 = {
    "type": "registry",
    "url": "http://wikidata.example.com/",
    "metadata": {},
}

metadata_fetcher = {
    "name": "swh-deposit",
    "version": "0.0.1",
    "metadata": {"sword_version": "2"},
}
metadata_fetcher2 = {
    "name": "swh-example",
    "version": "0.0.1",
    "metadata": {},
}

date_visit1 = datetime.datetime(2015, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
type_visit1 = "git"

date_visit2 = datetime.datetime(2017, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
type_visit2 = "hg"

date_visit3 = datetime.datetime(2018, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
type_visit3 = "deb"

release = {
    "id": hash_to_bytes("a673e617fcc6234e29b2cad06b8245f96c415c61"),
    "name": b"v0.0.1",
    "author": {
        "name": b"olasd",
        "email": b"nic@olasd.fr",
        "fullname": b"olasd <nic@olasd.fr>",
    },
    "date": {
        "timestamp": {"seconds": 1234567890, "microseconds": 0},
        "offset": 42,
        "negative_utc": False,
    },
    "target": b"43210987654321098765",
    "target_type": "revision",
    "message": b"synthetic release",
    "synthetic": True,
}

release2 = {
    "id": hash_to_bytes("6902bd4c82b7d19a421d224aedab2b74197e420d"),
    "name": b"v0.0.2",
    "author": {
        "name": b"tony",
        "email": b"ar@dumont.fr",
        "fullname": b"tony <ar@dumont.fr>",
    },
    "date": {
        "timestamp": {"seconds": 1634366813, "microseconds": 0},
        "offset": -120,
        "negative_utc": False,
    },
    "target": b"432109\xa9765432\xc309\x00765",
    "target_type": "revision",
    "message": b"v0.0.2\nMisc performance improvements + bug fixes",
    "synthetic": False,
}

release3 = {
    "id": hash_to_bytes("3e9050196aa288264f2a9d279d6abab8b158448b"),
    "name": b"v0.0.2",
    "author": {
        "name": b"tony",
        "email": b"tony@ardumont.fr",
        "fullname": b"tony <tony@ardumont.fr>",
    },
    "date": {
        "timestamp": {"seconds": 1634336813, "microseconds": 0},
        "offset": 0,
        "negative_utc": False,
    },
    "target": hash_to_bytes("df7a6f6a99671fb7f7343641aff983a314ef6161"),
    "target_type": "revision",
    "message": b"yet another synthetic release",
    "synthetic": True,
}

releases = (release, release2, release3)


snapshot = {
    "id": hash_to_bytes("409ee1ff3f10d166714bc90581debfd0446dda57"),
    "branches": {
        b"master": {
            "target": hash_to_bytes("066b1b62dbfa033362092af468bf6cfabec230e7"),
            "target_type": "revision",
        },
    },
}

empty_snapshot = {
    "id": hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"),
    "branches": {},
}

complete_snapshot = {
    "id": hash_to_bytes("a56ce2d81c190023bb99a3a36279307522cb85f6"),
    "branches": {
        b"directory": {
            "target": hash_to_bytes("1bd0e65f7d2ff14ae994de17a1e7fe65111dcad8"),
            "target_type": "directory",
        },
        b"directory2": {
            "target": hash_to_bytes("1bd0e65f7d2ff14ae994de17a1e7fe65111dcad8"),
            "target_type": "directory",
        },
        b"content": {
            "target": hash_to_bytes("fe95a46679d128ff167b7c55df5d02356c5a1ae1"),
            "target_type": "content",
        },
        b"alias": {"target": b"revision", "target_type": "alias",},
        b"revision": {
            "target": hash_to_bytes("aafb16d69fd30ff58afdd69036a26047f3aebdc6"),
            "target_type": "revision",
        },
        b"release": {
            "target": hash_to_bytes("7045404f3d1c54e6473c71bbb716529fbad4be24"),
            "target_type": "release",
        },
        b"snapshot": {
            "target": hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"),
            "target_type": "snapshot",
        },
        b"dangling": None,
    },
}

snapshots = (snapshot, empty_snapshot, complete_snapshot)


origin_metadata = {
    "origin_url": origin["url"],
    "discovery_date": datetime.datetime(
        2015, 1, 1, 21, 0, 0, tzinfo=datetime.timezone.utc
    ),
    "authority": {
        "type": metadata_authority["type"],
        "url": metadata_authority["url"],
    },
    "fetcher": {
        "name": metadata_fetcher["name"],
        "version": metadata_fetcher["version"],
    },
    "format": "json",
    "metadata": b'{"foo": "bar"}',
}
origin_metadata2 = {
    "origin_url": origin["url"],
    "discovery_date": datetime.datetime(
        2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
    ),
    "authority": {
        "type": metadata_authority["type"],
        "url": metadata_authority["url"],
    },
    "fetcher": {
        "name": metadata_fetcher["name"],
        "version": metadata_fetcher["version"],
    },
    "format": "yaml",
    "metadata": b"foo: bar",
}
origin_metadata3 = {
    "origin_url": origin["url"],
    "discovery_date": datetime.datetime(
        2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
    ),
    "authority": {
        "type": metadata_authority2["type"],
        "url": metadata_authority2["url"],
    },
    "fetcher": {
        "name": metadata_fetcher2["name"],
        "version": metadata_fetcher2["version"],
    },
    "format": "yaml",
    "metadata": b"foo: bar",
}

person = {
    "name": b"John Doe",
    "email": b"john.doe@institute.org",
    "fullname": b"John Doe <john.doe@institute.org>",
}

objects = {
    "content": contents,
    "skipped_content": skipped_contents,
    "directory": directories,
    "revision": revisions,
    "origin": origins,
    "release": releases,
    "snapshot": snapshots,
}
