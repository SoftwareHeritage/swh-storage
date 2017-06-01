insert into entity_history
  (uuid, parent, name, type, description, homepage, active, generated, validity)
values
  ('5f4d4c51-498a-4e28-88b3-b3e4e8396cba', NULL, 'softwareheritage',
   'organization', 'Software Heritage',
   'http://www.softwareheritage.org/', true, false, ARRAY[now()]),
  ('6577984d-64c8-4fab-b3ea-3cf63ebb8589', NULL, 'gnu', 'organization',
   'GNU is not UNIX', 'https://gnu.org/', true, false, ARRAY[now()]),
  ('7c33636b-8f11-4bda-89d9-ba8b76a42cec', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Hosting', 'group_of_entities',
   'GNU Hosting facilities', NULL, true, false, ARRAY[now()]),
  ('4706c92a-8173-45d9-93d7-06523f249398', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU rsync mirror', 'hosting',
   'GNU rsync mirror', 'rsync://mirror.gnu.org/', true, false, ARRAY[now()]),
  ('5cb20137-c052-4097-b7e9-e1020172c48e', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Projects', 'group_of_entities',
   'GNU Projects', 'https://gnu.org/software/', true, false, ARRAY[now()]),
  ('4bfb38f6-f8cd-4bc2-b256-5db689bb8da4', NULL, 'GitHub', 'organization',
   'GitHub', 'https://github.org/', true, false, ARRAY[now()]),
  ('aee991a0-f8d7-4295-a201-d1ce2efc9fb2', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Hosting', 'group_of_entities',
   'GitHub Hosting facilities', 'https://github.org/', true, false, ARRAY[now()]),
  ('34bd6b1b-463f-43e5-a697-785107f598e4', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub git hosting', 'hosting',
   'GitHub git hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('e8c3fc2e-a932-4fd7-8f8e-c40645eb35a7', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub asset hosting', 'hosting',
   'GitHub asset hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('9f7b34d9-aa98-44d4-8907-b332c1036bc3', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Organizations', 'group_of_entities',
   'GitHub Organizations', 'https://github.org/', true, false, ARRAY[now()]),
  ('ad6df473-c1d2-4f40-bc58-2b091d4a750e', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Users', 'group_of_entities',
   'GitHub Users', 'https://github.org/', true, false, ARRAY[now()]);

insert into listable_entity
  (uuid, list_engine)
values
  ('34bd6b1b-463f-43e5-a697-785107f598e4', 'swh.lister.github');

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
  ('Zveno'),
  ('Affero-possibility'),
  ('Apache-possibility'),
  ('Apache_v2-possibility'),
  ('Artistic-possibility'),
  ('BSD-possibility'),
  ('CMU-possibility'),
  ('CPL-possibility'),
  ('Freeware'),
  ('FSF-possibility'),
  ('GPL-2.0+:3.0'),
  ('GPL-2.0+&GPL-3.0+'),
  ('GPL-2.1[sic]'),
  ('GPL-2.1+[sic]'),
  ('GPL-possibility'),
  ('HP-possibility'),
  ('IBM-possibility'),
  ('ISC-possibility'),
  ('LGPL-possibility'),
  ('LGPL_v3-possibility'),
  ('Microsoft-possibility'),
  ('MIT-possibility'),
  ('NOT-public-domain'),
  ('Perl-possibility'),
  ('PHP-possibility'),
  ('RSA-possibility'),
  ('Sun-possibility'),
  ('Trademark-ref'),
  ('UnclassifiedLicense'),
  ('W3C-possibility'),
  ('X11-possibility');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('nomos', '3.1.0rc2-31-ga2cbb8c', '{"command_line": "nomossa <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '5.22', '{"command_line": "file --mime <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('universal-ctags', '~git7859817b', '{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments", "max_content_size": 10240}');
