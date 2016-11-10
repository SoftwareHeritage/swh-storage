-- SWH DB schema upgrade
-- from_version: 89
-- to_version: 90
-- description: indexer: Add content_ctags

insert into dbversion(version, release, description)
      values(90, now(), 'Work In Progress');

comment on type content_status is 'Content visibility';
comment on type entity_type is 'Entity types';
comment on type revision_type is 'Possible revision types';
comment on type object_type is 'Data object types stored in data model';
comment on type languages is 'Languages recognized by language indexer';
comment on type ctags_languages is 'Languages recognized by ctags indexer';

create table fossology_license(
  id smallserial primary key,
  name text not null
);

comment on table fossology_license is 'Possible license recognized by license indexer';
comment on column fossology_license.id is 'License identifier';
comment on column fossology_license.name is 'License name';

create unique index on fossology_license(name);

create table indexer_configuration (
  id serial primary key not null,
  tool_name text not null,
  tool_version text not null,
  tool_configuration jsonb
);

comment on table indexer_configuration is 'Indexer''s configuration version';
comment on column indexer_configuration.id is 'Tool identifier';
comment on column indexer_configuration.tool_version is 'Tool name';
comment on column indexer_configuration.tool_version is 'Tool version';
comment on column indexer_configuration.tool_configuration is 'Tool configuration: command line, flags, etc...';

create unique index on indexer_configuration(tool_name, tool_version);

create table content_fossology_license (
  id sha1 references content(sha1) not null,
  license_id smallserial references fossology_license(id) not null,
  indexer_configuration_id bigserial references indexer_configuration(id) not null
);

create unique index on content_fossology_license(id, license_id, indexer_configuration_id);

comment on table content_fossology_license is 'license associated to a raw content';
comment on column content_fossology_license.id is 'Raw content identifier';
comment on column content_fossology_license.license_id is 'One of the content''s license identifier';

-- data

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('nomos', '3.1.0rc2-31-ga2cbb8c', '{"command_line": "nomossa"}');

insert into fossology_license (name)
values
  ('3DFX'),
  ('3DFX-PL'),
  ('AAL'),
  ('ACAA'),
  ('ACDL'),
  ('ACE'),
  ('Adaptec-GPL'),
  ('Adaptec.RESTRICTED'),
  ('Adobe'),
  ('Adobe-AFM'),
  ('Adobe-EULA'),
  ('Adobe-SCLA'),
  ('AFL'),
  ('AFL-1.0'),
  ('AFL-1.1'),
  ('AFL-1.2'),
  ('AFL-2.0'),
  ('AFL-2.1'),
  ('AFL-3.0'),
  ('AFPL-Ghostscript'),
  ('AgainstDRM'),
  ('AGE-Logic'),
  ('Agere-EULA'),
  ('AGFA-EULA'),
  ('AGFA(RESTRICTED)'),
  ('AGPL'),
  ('AGPL-1.0'),
  ('AGPL-1.0+'),
  ('AGPL-3.0'),
  ('AGPL-3.0+'),
  ('AGFA.RESTRICTED'),
  ('Aladdin'),
  ('Alfresco'),
  ('Alfresco-FLOSS'),
  ('Algorithmics'),
  ('AMD'),
  ('AMD-EULA'),
  ('AML'),
  ('AndroidFraunhofer.Commercial'),
  ('AndroidSDK.Commercial'),
  ('ANTLR-PD'),
  ('AOL-EULA'),
  ('Apache'),
  ('Apache-1.0'),
  ('Apache-1.1'),
  ('Apache-2.0'),
  ('Apache-style'),
  ('Apache-v1.0'),
  ('APL'),
  ('APL-1.0'),
  ('Apple'),
  ('Apple-EULA'),
  ('Apple.FontForge'),
  ('Apple.Sample'),
  ('APSL'),
  ('APSL-1.0'),
  ('APSL-1.1'),
  ('APSL-1.2'),
  ('APSL-2.0'),
  ('APSL-style'),
  ('Aptana'),
  ('Aptana-1.0'),
  ('ARJ'),
  ('Arphic-Font-PL'),
  ('Arphic-style'),
  ('Artifex'),
  ('Artistic-1.0'),
  ('Artistic-1.0-cl8'),
  ('Artistic-1.0-Perl'),
  ('Artistic-2.0'),
  ('Ascender-EULA'),
  ('ATI-EULA'),
  ('ATMEL-FW'),
  ('ATT'),
  ('ATT.Non-commercial'),
  ('ATT-Source'),
  ('ATT-Source-1.0'),
  ('ATT-Source-1.2d'),
  ('ATT-style'),
  ('AVM'),
  ('Baekmuk-Font'),
  ('Baekmuk.Hwan'),
  ('BancTec'),
  ('BEA'),
  ('Beerware'),
  ('Bellcore'),
  ('Bellcore-style'),
  ('BH-Font'),
  ('BH-Font-style'),
  ('BISON'),
  ('Bitstream'),
  ('BitTorrent'),
  ('BitTorrent-1.0'),
  ('BitTorrent-1.1'),
  ('BIZNET'),
  ('BIZNET-style'),
  ('BrainStorm-EULA'),
  ('Broadcom.Commercial'),
  ('Broadcom-EULA'),
  ('BSD'),
  ('BSD-2-Clause'),
  ('BSD-2-Clause-FreeBSD'),
  ('BSD-2-Clause-NetBSD'),
  ('BSD-3-Clause'),
  ('BSD-3-Clause-Clear'),
  ('BSD-3-Clause-Severability'),
  ('BSD-4-Clause'),
  ('BSD-4-Clause-UC'),
  ('BSD.non-commercial'),
  ('BSD-style'),
  ('BSL-1.0'),
  ('BSL-style'),
  ('CA'),
  ('Cadence'),
  ('Catharon'),
  ('CATOSL'),
  ('CATOSL-1.1'),
  ('CC0-1.0'),
  ('CC-BY'),
  ('CC-BY-1.0'),
  ('CC-BY-2.0'),
  ('CC-BY-2.5'),
  ('CC-BY-3.0'),
  ('CC-BY-4.0'),
  ('CC-BY-NC-1.0'),
  ('CC-BY-NC-2.0'),
  ('CC-BY-NC-2.5'),
  ('CC-BY-NC-3.0'),
  ('CC-BY-NC-4.0'),
  ('CC-BY-NC-ND-1.0'),
  ('CC-BY-NC-ND-2.0'),
  ('CC-BY-NC-ND-2.5'),
  ('CC-BY-NC-ND-3.0'),
  ('CC-BY-NC-ND-4.0'),
  ('CC-BY-NC-SA-1.0'),
  ('CC-BY-NC-SA-2.0'),
  ('CC-BY-NC-SA-2.5'),
  ('CC-BY-NC-SA-3.0'),
  ('CC-BY-NC-SA-4.0'),
  ('CC-BY-ND-1.0'),
  ('CC-BY-ND-2.0'),
  ('CC-BY-ND-2.5'),
  ('CC-BY-ND-3.0'),
  ('CC-BY-ND-4.0'),
  ('CC-BY-SA'),
  ('CC-BY-SA-1.0'),
  ('CC-BY-SA-2.0'),
  ('CC-BY-SA-2.5'),
  ('CC-BY-SA-3.0'),
  ('CC-BY-SA-4.0'),
  ('CC-LGPL'),
  ('CC-LGPL-2.1'),
  ('CCLRC'),
  ('CCPL'),
  ('CDDL'),
  ('CDDL-1.0'),
  ('CDDL-1.1'),
  ('CECILL'),
  ('CECILL-1.0'),
  ('CECILL-1.1'),
  ('CECILL-2.0'),
  ('CECILL-B'),
  ('CECILL-C'),
  ('CECILL(dual)'),
  ('Cisco'),
  ('Cisco-style'),
  ('Citrix'),
  ('ClArtistic'),
  ('ClearSilver'),
  ('CMake'),
  ('CMU'),
  ('CMU-style'),
  ('CNRI-Python'),
  ('CNRI-Python-GPL-Compatible'),
  ('Combined_OpenSSL+SSLeay'),
  ('COMMERCIAL'),
  ('CompuServe'),
  ('Comtrol'),
  ('Condor-1.0'),
  ('Condor-1.1'),
  ('CopyLeft[1]'),
  ('CopyLeft[2]'),
  ('CPAL'),
  ('CPAL-1.0'),
  ('CPL'),
  ('CPL-0.5'),
  ('CPL-1.0'),
  ('CPOL'),
  ('CPOL-1.02'),
  ('Cryptogams'),
  ('CUA-OPL-1.0'),
  ('CUPS'),
  ('CUPS-EULA'),
  ('Cygnus-eCos-1.0'),
  ('Cylink-ISC'),
  ('Cypress-FW'),
  ('DARPA'),
  ('DARPA-Cougaar'),
  ('Debian-social-DFSG'),
  ('Debian-SPI'),
  ('Debian-SPI-style'),
  ('D.E.Knuth'),
  ('D-FSL-1.0'),
  ('DMTF'),
  ('DOCBOOK'),
  ('DOCBOOK-style'),
  ('DPTC'),
  ('DSCT'),
  ('DSL'),
  ('Dual-license'),
  ('Dyade'),
  ('EBT-style'),
  ('ECL-1.0'),
  ('ECL-2.0'),
  ('eCos-2.0'),
  ('EDL-1.0'),
  ('EFL'),
  ('EFL-1.0'),
  ('EFL-2.0'),
  ('eGenix'),
  ('Entessa'),
  ('Epinions'),
  ('EPL'),
  ('EPL-1.0'),
  ('Epson-EULA'),
  ('Epson-PL'),
  ('ErlPL-1.1'),
  ('EUDatagrid'),
  ('EUPL-1.0'),
  ('EUPL-1.1'),
  ('FaCE'),
  ('Fair'),
  ('Fair-style'),
  ('FAL-1.0'),
  ('FAL-1.3'),
  ('Fedora'),
  ('FedoraCLA'),
  ('Flash2xml-1.0'),
  ('Flora'),
  ('Flora-1.0'),
  ('Flora-1.1'),
  ('Frameworx'),
  ('Frameworx-1.0'),
  ('FreeBSD-Doc'),
  ('Free-PL'),
  ('Free-SW'),
  ('Free-SW.run-COMMAND'),
  ('FSF'),
  ('FTL'),
  ('FTL-style'),
  ('Fujitsu'),
  ('Garmin-EULA'),
  ('GFDL'),
  ('GFDL-1.1'),
  ('GFDL-1.1+'),
  ('GFDL-1.2'),
  ('GFDL-1.2+'),
  ('GFDL-1.3'),
  ('GFDL-v1.2'),
  ('Genivia.Commercial'),
  ('Ghostscript-GPL'),
  ('Ghostscript-GPL-1.1'),
  ('Giftware'),
  ('GNU-copyleft'),
  ('GNU-Ghostscript'),
  ('GNU-javamail-exception'),
  ('GNU-Manpages'),
  ('GNU-style.EXECUTE'),
  ('GNU-style.interactive'),
  ('Google'),
  ('Google-BSD'),
  ('Govt-restrict'),
  ('Govt-rights'),
  ('Govt-work'),
  ('GPDL'),
  ('GPL'),
  ('GPL-1.0'),
  ('GPL-1.0+'),
  ('GPL-2.0'),
  ('GPL-2.0+'),
  ('GPL-2.0-with-autoconf-exception'),
  ('GPL-2.0-with-bison-exception'),
  ('GPL-2.0+-with-bison-exception'),
  ('GPL-2.0-with-classpath-exception'),
  ('GPL-2.0+-with-classpath-exception'),
  ('GPL-2.0-with-font-exception'),
  ('GPL-2.0-with-GCC-exception'),
  ('GPL-2.0-with-trolltech-exception'),
  ('GPL-2.0+-with-UPX-exception'),
  ('GPL-3.0'),
  ('GPL-3.0+'),
  ('GPL-3.0-with-autoconf-exception'),
  ('GPL-3.0+-with-autoconf-exception'),
  ('GPL-3.0-with-bison-exception'),
  ('GPL-3.0+-with-bison-exception'),
  ('GPL-3.0-with-classpath-exception'),
  ('GPL-3.0+-with-classpath-exception'),
  ('GPL-3.0-with-GCC-exception'),
  ('GPL-3.0+-with-GCC-exception'),
  ('GPL-exception'),
  ('GPL-or-LGPL'),
  ('GPL(rms)'),
  ('GPL-with-autoconf-exception'),
  ('gSOAP'),
  ('gSOAP-1.3b'),
  ('H2'),
  ('H2-1.0'),
  ('Hacktivismo'),
  ('Hauppauge'),
  ('Helix.RealNetworks-EULA'),
  ('HP'),
  ('HP-Compaq'),
  ('HP-DEC'),
  ('HP-DEC-style'),
  ('HP-EULA'),
  ('HP+IBM'),
  ('HPND'),
  ('HP-Proprietary'),
  ('HP-style'),
  ('HSQLDB'),
  ('IBM'),
  ('IBM-Courier'),
  ('IBM-EULA'),
  ('IBM-JCL'),
  ('IBM-pibs'),
  ('IBM-reciprocal'),
  ('ICU'),
  ('ID-EULA'),
  ('IDPL'),
  ('IDPL-1.0'),
  ('IEEE-Doc'),
  ('IETF'),
  ('IETF-style'),
  ('IJG'),
  ('ImageMagick'),
  ('ImageMagick-style'),
  ('Imlib2'),
  ('InfoSeek'),
  ('info-zip'),
  ('InnerNet'),
  ('InnerNet-2.00'),
  ('InnerNet-style'),
  ('Intel'),
  ('Intel.Commercial'),
  ('Intel-EULA'),
  ('Intel-other'),
  ('Intel.RESTRICTED'),
  ('Intel-WLAN'),
  ('Interbase-1.0'),
  ('Interbase-PL'),
  ('Interlink-EULA'),
  ('Intranet-only'),
  ('IOS'),
  ('IoSoft.COMMERCIAL'),
  ('IPA'),
  ('IPA-Font-EULA'),
  ('IP-claim'),
  ('IPL'),
  ('IPL-1.0'),
  ('IPL-2.0'),
  ('IPTC'),
  ('IronDoc'),
  ('ISC'),
  ('Jabber'),
  ('Jabber-1.0'),
  ('Java-Multi-Corp'),
  ('Java-WSDL4J'),
  ('Java-WSDL-Policy'),
  ('Java-WSDL-Schema'),
  ('Java-WSDL-Spec'),
  ('JISP'),
  ('JPEG.netpbm'),
  ('JPNIC'),
  ('JSON'),
  ('KDE'),
  ('KD-Tools-EULA'),
  ('Keyspan-FW'),
  ('KnowledgeTree-1.1'),
  ('Knuth-style'),
  ('Lachman-Proprietary'),
  ('Larabie-EULA'),
  ('LDP'),
  ('LDP-1A'),
  ('LDP-2.0'),
  ('Legato'),
  ('Leptonica'),
  ('LGPL'),
  ('LGPL-1.0'),
  ('LGPL-1.0+'),
  ('LGPL-2.0'),
  ('LGPL-2.0+'),
  ('LGPL-2.1'),
  ('LGPL-2.1+'),
  ('LGPL-3.0'),
  ('LGPL-3.0+'),
  ('LIBGCJ'),
  ('Libpng'),
  ('Link-exception'),
  ('LinuxDoc'),
  ('Linux-HOWTO'),
  ('Logica-OSL-1.0'),
  ('LPL-1.0'),
  ('LPL-1.02'),
  ('LPPL'),
  ('LPPL-1.0'),
  ('LPPL-1.0+'),
  ('LPPL-1.1'),
  ('LPPL-1.1+'),
  ('LPPL-1.2'),
  ('LPPL-1.2+'),
  ('LPPL-1.3'),
  ('LPPL-1.3+'),
  ('LPPL-1.3a'),
  ('LPPL-1.3a+'),
  ('LPPL-1.3b'),
  ('LPPL-1.3b+'),
  ('LPPL-1.3c'),
  ('LPPL-1.3c+'),
  ('MacroMedia-RPSL'),
  ('Macrovision'),
  ('Macrovision-EULA'),
  ('Majordomo'),
  ('Majordomo-1.1'),
  ('Mandriva'),
  ('Mellanox'),
  ('MetroLink'),
  ('MetroLink-nonfree'),
  ('Mibble'),
  ('Mibble-2.8'),
  ('Microsoft'),
  ('Migemo'),
  ('MindTerm'),
  ('MirOS'),
  ('MIT'),
  ('MIT.BSD'),
  ('MIT&BSD'),
  ('MITEM'),
  ('Mitre'),
  ('MitreCVW'),
  ('MitreCVW-style'),
  ('MIT-style'),
  ('Motorola'),
  ('Motosoto'),
  ('MPEG3-decoder'),
  ('MPL'),
  ('MPL-1.0'),
  ('MPL-1.1'),
  ('MPL-1.1+'),
  ('MPL-1.1-style'),
  ('MPL-2.0'),
  ('MPL-2.0-no-copyleft-exception'),
  ('MPL-EULA-1.1'),
  ('MPL-EULA-2.0'),
  ('MPL-EULA-3.0'),
  ('MPL-style'),
  ('MPL.TPL'),
  ('MPL.TPL-1.0'),
  ('M-Plus-Project'),
  ('MRL'),
  ('MS-EULA'),
  ('MS-indemnity'),
  ('MS-IP'),
  ('MS-LPL'),
  ('MS-LRL'),
  ('MS-PL'),
  ('MS-RL'),
  ('MS-SSL'),
  ('Multics'),
  ('MX4J'),
  ('MX4J-1.0'),
  ('MySQL-0.3'),
  ('MySQL.FLOSS'),
  ('MySQL-style'),
  ('NASA'),
  ('NASA-1.3'),
  ('Naumen'),
  ('NBPL-1.0'),
  ('nCipher'),
  ('NCSA'),
  ('NESSUS-EULA'),
  ('NGPL'),
  ('Nokia'),
  ('No_license_found'),
  ('non-ATT-BSD'),
  ('Non-commercial'),
  ('Non-profit'),
  ('NOSL'),
  ('NOSL-1.0'),
  ('Not-for-sale'),
  ('Not-Free'),
  ('Not-Internet'),
  ('Not-OpenSource'),
  ('NOT-Open-Source'),
  ('NotreDame'),
  ('NotreDame-style'),
  ('Novell'),
  ('Novell-EULA'),
  ('Novell-IP'),
  ('NPL'),
  ('NPL-1.0'),
  ('NPL-1.1'),
  ('NPL-1.1+'),
  ('NPL-EULA'),
  ('NPOSL-3.0'),
  ('NRL'),
  ('NTP'),
  ('Nvidia'),
  ('Nvidia-EULA'),
  ('OASIS'),
  ('OCL'),
  ('OCL-1.0'),
  ('OCLC'),
  ('OCLC-1.0'),
  ('OCLC-2.0'),
  ('OCL-style'),
  ('ODbL-1.0'),
  ('ODL'),
  ('OFL-1.0'),
  ('OFL-1.1'),
  ('OGTSL'),
  ('OLDAP'),
  ('OLDAP-1.1'),
  ('OLDAP-1.2'),
  ('OLDAP-1.3'),
  ('OLDAP-1.4'),
  ('OLDAP-2.0'),
  ('OLDAP-2.0.1'),
  ('OLDAP-2.1'),
  ('OLDAP-2.2'),
  ('OLDAP-2.2.1'),
  ('OLDAP-2.2.2'),
  ('OLDAP-2.3'),
  ('OLDAP-2.4'),
  ('OLDAP-2.5'),
  ('OLDAP-2.6'),
  ('OLDAP-2.7'),
  ('OLDAP-2.8'),
  ('OLDAP-style'),
  ('OMF'),
  ('OMRON'),
  ('Ontopia'),
  ('OpenCASCADE-PL'),
  ('OpenGroup'),
  ('OpenGroup-Proprietary'),
  ('OpenGroup-style'),
  ('OpenMap'),
  ('OpenMarket'),
  ('Open-PL'),
  ('Open-PL-0.4'),
  ('Open-PL-1.0'),
  ('Open-PL-style'),
  ('OpenSSL'),
  ('OpenSSL-exception'),
  ('OPL-1.0'),
  ('OPL-style'),
  ('Oracle-Berkeley-DB'),
  ('Oracle-Dev'),
  ('Oracle-EULA'),
  ('OReilly'),
  ('OReilly-style'),
  ('OSD'),
  ('OSF'),
  ('OSF-style'),
  ('OSL'),
  ('OSL-1.0'),
  ('OSL-1.1'),
  ('OSL-2.0'),
  ('OSL-2.1'),
  ('OSL-3.0'),
  ('Paradigm'),
  ('Patent-ref'),
  ('PDDL-1.0'),
  ('Phorum'),
  ('PHP'),
  ('PHP-2.0'),
  ('PHP-2.0.2'),
  ('PHP-3.0'),
  ('PHP-3.01'),
  ('PHP-style'),
  ('Piriform'),
  ('Pixware-EULA'),
  ('Platform-Computing(RESTRICTED)'),
  ('Polyserve-CONFIDENTIAL'),
  ('Postfix'),
  ('PostgreSQL'),
  ('Powder-Proprietary'),
  ('Princeton'),
  ('Princeton-style'),
  ('Proprietary'),
  ('Public-domain'),
  ('Public-domain(C)'),
  ('Public-domain-ref'),
  ('Public-Use'),
  ('Public-Use-1.0'),
  ('Python'),
  ('Python-2.0'),
  ('Python-2.0.1'),
  ('Python-2.0.2'),
  ('Python-2.1.1'),
  ('Python-2.1.3'),
  ('Python-2.2'),
  ('Python-2.2.3'),
  ('Python-2.2.7'),
  ('Python-2.3'),
  ('Python-2.3.7'),
  ('Python-2.4.4'),
  ('Python-style'),
  ('Qmail'),
  ('QPL'),
  ('QPL-1.0'),
  ('QT.Commercial'),
  ('QuarterDeck'),
  ('Quest-EULA'),
  ('RCSL'),
  ('RCSL-1.0'),
  ('RCSL-2.0'),
  ('RCSL-3.0'),
  ('RealNetworks-EULA'),
  ('RedHat'),
  ('RedHat-EULA'),
  ('RedHat.Non-commercial'),
  ('RedHat-specific'),
  ('Redland'),
  ('Restricted-rights'),
  ('RHeCos-1.1'),
  ('Riverbank-EULA'),
  ('RPL'),
  ('RPL-1.0'),
  ('RPL-1.1'),
  ('RPL-1.5'),
  ('RPSL'),
  ('RPSL-1.0'),
  ('RPSL-2.0'),
  ('RPSL-3.0'),
  ('RSA-DNS'),
  ('RSA-Security'),
  ('RSCPL'),
  ('Ruby'),
  ('Same-license-as'),
  ('SAX-PD'),
  ('SciTech'),
  ('SCO.commercial'),
  ('SCSL'),
  ('SCSL-2.3'),
  ('SCSL-3.0'),
  ('SCSL-TSA'),
  ('SCSL-TSA-1.0'),
  ('See-doc.OTHER'),
  ('See-file'),
  ('See-file.COPYING'),
  ('See-file.LICENSE'),
  ('See-file.README'),
  ('See-URL'),
  ('Sendmail'),
  ('SGI'),
  ('SGI-B-1.0'),
  ('SGI-B-1.1'),
  ('SGI-B-2.0'),
  ('SGI-Freeware'),
  ('SGI_GLX'),
  ('SGI_GLX-1.0'),
  ('SGI-Proprietary'),
  ('SGI-style'),
  ('SGML'),
  ('SimPL-2.0'),
  ('SISSL'),
  ('SISSL-1.1'),
  ('SISSL-1.2'),
  ('Skype-EULA'),
  ('Sleepycat'),
  ('Sleepycat.Non-commercial'),
  ('SMLNJ'),
  ('SNIA'),
  ('SNIA-1.0'),
  ('SNIA-1.1'),
  ('SpikeSource'),
  ('SPL'),
  ('SPL-1.0'),
  ('Stanford'),
  ('Stanford-style'),
  ('SugarCRM-1.1.3'),
  ('Sun'),
  ('SUN'),
  ('Sun-BCLA'),
  ('Sun-BCLA-1.5.0'),
  ('Sun-EULA'),
  ('Sun-IP'),
  ('Sun-Java'),
  ('Sun.Non-commercial'),
  ('SunPro'),
  ('Sun-Proprietary'),
  ('Sun.RESTRICTED'),
  ('Sun-RPC'),
  ('Sun-SCA'),
  ('Sun(tm)'),
  ('SW-Research'),
  ('Tapjoy'),
  ('TCL'),
  ('Tektronix'),
  ('Tektronix-style'),
  ('TeX-exception'),
  ('Trident-EULA'),
  ('Trolltech'),
  ('TrueCrypt-3.0'),
  ('U-BC'),
  ('U-Cambridge'),
  ('U-Cambridge-style'),
  ('UCAR'),
  ('UCAR-style'),
  ('U-Chicago'),
  ('U-Columbia'),
  ('UCWare-EULA'),
  ('U-Del'),
  ('U-Del-style'),
  ('U-Edinburgh'),
  ('U-Edinburgh-style'),
  ('U-Michigan'),
  ('U-Mich-style'),
  ('U-Monash'),
  ('Unicode'),
  ('Unidex'),
  ('UnitedLinux-EULA'),
  ('Unix-Intl'),
  ('Unlicense'),
  ('unRAR restriction'),
  ('URA.govt'),
  ('USC'),
  ('USC.Non-commercial'),
  ('USC-style'),
  ('US-Export-restrict'),
  ('USL-Europe'),
  ('U-Utah'),
  ('U-Wash.Free-Fork'),
  ('U-Washington'),
  ('U-Wash-style'),
  ('VIM'),
  ('Vixie'),
  ('Vixie-license'),
  ('VMware-EULA'),
  ('VSL-1.0'),
  ('W3C'),
  ('W3C-IP'),
  ('W3C-style'),
  ('Wash-U-StLouis'),
  ('Wash-U-style'),
  ('Watcom'),
  ('Watcom-1.0'),
  ('WebM'),
  ('Wintertree'),
  ('WordNet-3.0'),
  ('WTFPL'),
  ('WTI.Not-free'),
  ('WXwindows'),
  ('X11'),
  ('X11-style'),
  ('Xerox'),
  ('Xerox-style'),
  ('XFree86'),
  ('XFree86-1.0'),
  ('XFree86-1.1'),
  ('Ximian'),
  ('Ximian-1.0'),
  ('XMLDB-1.0'),
  ('Xnet'),
  ('X/Open'),
  ('XOPEN-EULA'),
  ('X/Open-style'),
  ('Yahoo-EULA'),
  ('YaST.SuSE'),
  ('YPL'),
  ('YPL-1.0'),
  ('YPL-1.1'),
  ('Zend-1.0'),
  ('Zend-2.0'),
  ('Zeus'),
  ('Zimbra'),
  ('Zimbra-1.2'),
  ('Zimbra-1.3'),
  ('Zlib'),
  ('Zlib-possibility'),
  ('ZoneAlarm-EULA'),
  ('ZPL'),
  ('ZPL-1.0'),
  ('ZPL-1.1'),
  ('ZPL-2.0'),
  ('ZPL-2.1'),
  ('Zveno');

-- create a temporary table for content_fossology_license tmp_content_fossology_license,
create or replace function swh_mktemp_content_fossology_license()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license (
    id           sha1,
    tool_name    text,
    tool_version text,
    license      text
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license() is 'Helper table to add content license';

-- create a temporary table for checking licenses' name
create or replace function swh_mktemp_content_fossology_license_unknown()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license_unknown (
    name       text not null
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license_unknown() is 'Helper table to list unknown licenses';


-- check which entries of tmp_bytea are missing from content_fossology_license
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_fossology_license_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_bytea as tmp
	 where not exists
	     (select 1 from content_fossology_license as c where c.id = tmp.id));
    return;
end
$$;

comment on function swh_content_fossology_license_missing() IS 'Filter missing content licenses';

-- add tmp_content_fossology_license entries to content_fossology_license, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_content_fossology_license_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_fossology_license), 1. COPY to
-- tmp_content_fossology_license, 2. call this function
create or replace function swh_content_fossology_license_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        delete from content_fossology_license
        where id in (select distinct id from tmp_content_fossology_license);
    end if;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          (select id from indexer_configuration where tool_name = tcl.tool_name
                                                and tool_version = tcl.tool_version)
                          as indexer_configuration_id
    from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do nothing;
    return;
end
$$;

comment on function swh_content_fossology_license_add(boolean) IS 'Add new content licenses';

create or replace function swh_content_fossology_license_unknown()
    returns setof text
    language plpgsql
as $$
begin
    return query
        select name from tmp_content_fossology_license_unknown t where not exists (
            select 1 from fossology_license where name=t.name
        );
end
$$;

comment on function swh_content_fossology_license_unknown() IS 'List unknown licenses';

create type content_fossology_license_signature as (
  id           sha1,
  tool_name    text,
  tool_version text,
  licenses     text[]
);

-- Retrieve list of content license from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_fossology_license_get()
    returns setof content_fossology_license_signature
    language plpgsql
as $$
begin
    return query
      select cl.id,
             ic.tool_name,
             ic.tool_version,
             array(select name
                   from fossology_license
                   where id = ANY(array_agg(cl.license_id))) as licenses
      from content_fossology_license cl
      inner join indexer_configuration ic on ic.id=cl.indexer_configuration_id
      group by cl.id, ic.tool_name, ic.tool_version;
    return;
end
$$;

comment on function swh_content_fossology_license_get() IS 'List content licenses';


-- ctags

drop function swh_content_ctags_add();

create or replace function swh_content_ctags_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        delete from content_ctags
        where id in (select distinct id from tmp_content_ctags);
    end if;

    insert into content_ctags (id, name, kind, line, lang)
    select id, name, kind, line, lang
    from tmp_content_ctags;
    return;
end
$$;

comment on function swh_content_ctags_add(boolean) IS 'Add ctags symbols per content';
