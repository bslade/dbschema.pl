#!/usr/bin/perl -w
#
#   $Id$
#
# dbschema.pl   A script to extract the DDL from a Sybase ASE.
#
# Home:         
#
# Written by:   Michael Peppler (mpeppler@peppler.org)
#               Substantially rewritten by David Whitmarsh from a partial
#               System 10 implementation by Ashu Joglekar
#               Major enhancements by David Owen (dowen@midsomer.org)
#               plus contributions from a large host of users and developers,
#               see the CHANGES file that should have come with this package.
#
# Maintainer:   None (formerly David Owen, dowen@midsomer.org)
#
# Last Mods:    3rd April 2009
#
# Release:      2.5
#
# Mailing List: No longer active
#
# Notes:        See CHANGES file for changes and recent mods.
#
# Usage:        See bottom of file for usage syntax or use "-h" option.
#
# Known Faults: o Still does not handle constraints *perfectly*.  Constraints
#                 referring to foreign databases definitely need a bit of
#                 work.  Probably best to have them extracted separately.
#                 Lots of new ASE 15 features probably don't work properly
#                 with this script.
#
# Changes:      See the CHANGES file.
#
#
#------------------------------------------------------------------------------


use strict;
use Sybase::DBlib;
use Getopt::Long;
use English;

sub checkPerms;
sub getPerms;
sub getObj;
sub getComment;
sub extractKeys;
sub printCols;
sub printIndexCols;
sub extractTables;
sub extractIndexes;
sub extractFkeys;
sub extractViews;
sub extractProcs;
sub extractTrigs;
sub extractRules;
sub extractTypes;
sub extractUsers;
sub extractGroups;
sub extractAliases;
sub extractLogins;
sub extractDiskInit;
sub extractCreateDB;
sub extractSegments;
sub dumpTable;
sub dumpView;
sub dumpIndex;
sub dumpDB;
sub switchScript;
sub initScriptFile;
sub verifyDirectories;
sub verifyDatabases;
sub buildDbDirs;
sub putScript;
sub logMessageInit;
sub logMessage;
sub logError;
sub usage;
sub showVersion;
sub getSrvVersion;
sub printSrvVersion;

my($VERSION) = "2.4.2";

my($dbproc, @dat);
my(%udflt, %urule);          # Global for a reason :-(
my($date);
my($suffix);
my($shell_suffix, $bcp_out, $bcp_in, %bcp_arg);
my($database);
my($srvVersion) = 0;
my($prefix);
my($multi_db_extract) = FALSE;
my($ex_db);
my($firstDatabase) = 1;   # Used for the very first run to ensure we have a correct prefix.

# Initialise the stats hash.  Next time do it per DB as well.

my(%Stats) = ('Tables'        => 0,
              'Procedures'    => 0,
              'Defaults'      => 0,
              'Rules'         => 0,
              'Triggers'      => 0,
              'Views'         => 0,
              'Permissions'   => 0,
              'Indexes'       => 0,
              'References'    => 0,
              'Bind Defaults' => 0,
              'Bind Rules'    => 0,
              'Types'         => 0,
              'Keys'          => 0,
              'Groups'        => 0,
              'Users'         => 0,
              'Aliases'       => 0,
              'Logins'        => 0);

# Command line variables.  I know that globals are horrible, but if I am
# going to remove them I might as well rewrite using CT-Lib.

my($User,
   $Server,
   $Password,
   @databases,
   $splitLevel,
   $prefix_opt,        # Contains master prefix string.
   $interfacesFile,
   $itemLike,
   $outTypes,
   $verbose,
   $verboseOutput,
   $extractSchema,
   $addDrops,
   $help,
   $allUserDatabases,
   $allDatabases,
   $generateBCP,
   $version,
   $separateIdxs,
   $separateTrigs,
   $cmdend,
   $for_load,
   $dsync,
   $cis,
   $for_asa,
   $stdin,             # Expect to read the password from stdin.
   $for_compare,
   $index_rebuild,
   $use_database,
   $comments,
   $setuser,
   $charset,
   $language,
   $packet_size,
   $force_defaults,
   $permissions,
   $target_db,
   $for_sql_server,
   $quiet,             # Overrides anymount of '-v's or '-verbose's :-)
   $references,
   @exclude_db,
   %bcpArguments,
   %bcpInArguments,
   %bcpOutArguments,
   $int_version);

$outTypes   = 'abcdefghijklmnopqrstuvwxyz';
$date       = scalar(localtime);
$verbose    = 0;       #  Must start off as 0.  -v's on the command line add to this value
                       #  0 => silent
                       #  1 => type level (default)
                       #  2 => object level
                       #  3 => user level debugging
                       # >4 => system level debugging

select (STDOUT); $| = 1;        # make unbuffered

#===================================#
# Process the command line options. #
#===================================#

if (defined(Getopt::Long::Configure)) {
    Getopt::Long::Configure("bundling", "no_ignore_case_always");
}
else {
    Getopt::Long::config("bundling", "no_ignore_case_always");
}

# --by-table

if (!GetOptions("U=s"              => \$User,
                "P:s"              => \$Password,
                "I=s"              => \$interfacesFile,
                "S=s"              => \$Server,
                "D=s@"             => \@databases,
                "a"                => \$allUserDatabases,
                "A"                => \$allDatabases,
                "b"                => \$generateBCP,
                "v+"               => \$verbose,
                "verbose+"         => \$verbose,
                "m"                => \$extractSchema,
                "k"                => \$addDrops,
                "o=s"              => \$prefix_opt,
                "O=i"              => \$splitLevel,
                "i"                => \$separateIdxs,
                "help|h"           => \$help,
                "V"                => \$verboseOutput,
                "t=s"              => \$itemLike,
                "version|q"        => \$version,
                "T=s"              => \$outTypes,
                "r"                => \$separateTrigs,
                "for-load"         => \$for_load,
                "for-compare"      => \$for_compare,
                "cis!"             => \$cis,
                "stdin"            => \$stdin,
                "use-database!"    => \$use_database,
                "target-db=s"      => \$target_db,
                "dsync=s"          => \$dsync,
                "comments!"        => \$comments,
                "setuser!"         => \$setuser,
                "permissions!"     => \$permissions,
                "force-defaults|F" => \$force_defaults,
                "charset|J=s"      => \$charset,
                "language|z=s"     => \$language,
                "packet-size=i"    => \$packet_size,
                "index-rebuild"    => \$index_rebuild,
                "cmdend|c=s"       => \$cmdend,
                "for-asa"          => \$for_asa,
                "for-sql-server"   => \$for_sql_server,
                "quiet"            => \$quiet,
                "references!"      => \$references,
                "exclude-db=s@"    => \@exclude_db,
                "X=s@"             => \@exclude_db,
                "bcp-arg:s%"       => \%bcpArguments,
                "bcp-in-arg:s%"    => \%bcpInArguments,
                "bcp-out-arg:s%"   => \%bcpOutArguments)) {
    usage();
    exit 1;
}

if ($help) {
    usage();
    exit 0;
}

if ($version) {
    showVersion();
    exit 0;
}

if ((scalar(@databases) > 0 && $allUserDatabases) ||
    (scalar(@databases) > 0 && $allDatabases)     ||
    ($allDatabases          && $allUserDatabases)) {
    die("-D, -a and -A are all mutually exclusive.\n");
}

$cmdend         = "go"          unless $cmdend;
$User           = whoami()      unless $User;
$suffix         = "sql";
$cis            = 1             unless defined($cis);
$use_database   = 1             unless defined($use_database);
$comments       = 1             unless defined($comments);
$setuser        = 1             unless defined($setuser);
$permissions    = 1             unless defined($permissions);
$references     = 1             unless defined($references);
#$prefix         = "script"      unless $prefix;
$itemLike       = "%"           unless $itemLike;
$Server         = $ENV{DSQUERY} unless $Server;
$for_load       = 0             unless $for_load;
$for_sql_server = 0             unless $for_sql_server;
$force_defaults = 0             unless $force_defaults;
$dsync          = "off"         unless $dsync;
$splitLevel     = 0             unless $splitLevel; # 0 => one file (the default)
                                                    # 1 => one file for each type
                                                    # 2 => each object in its own file
                                                    #      (where practical or possible)
                                                    # 3 => as 2 except group tables into
                                                    #      one logical unit.  (I wish!)

# Do this one last to override the '-v's

if ($quiet) {
    $verbose = 0;
}
else {
    # I increased the value of the comparison by 1 for each $verbose to
    # accomodate a $verbose == 0, so maintain backwards compatibility, the
    # default verbosity should be 1.
    ++$verbose;
}

# If we are splitting into separate files, then there is no point in
# wasting time trying to sort objects by dependency.  Gets around a bug in
# dumpView!

if ($splitLevel == 2) {
    $for_compare = 1;
}

# If the chap is extracting with a view to loading into M$ Sql Server, then
# he will not want a number of things, CIS being one.

if ($for_sql_server) {
    $cis = 0;
}

if ($dsync !~ /\b(on|off|true|false)\b/) {
    print STDERR "Option dsync: $dsync - Invalid option value.\n";
    usage();
    exit 0;
}

push(@databases, 'master') if (0 == @databases    &&
                               !$allUserDatabases &&
                               !$allDatabases);

# Vars for generating bcp scripts
$shell_suffix  = ($OSNAME !~ /^MSWin/ ? 'sh' : 'bat');

if ($generateBCP) {
    # %bcp_arg contains the option arguments for the bcp options -U -S -P
    #
    # Options are either taken from the options passed to dbschema or
    # environment variables.  Not sure if we should set the packetsize?

    $bcp_arg{BCP_OUT_S}  = $Server;
    $bcp_arg{BCP_OUT_U}  = $User;
    $bcp_arg{CHARSET}    = $charset        if $charset;
    $bcp_arg{LANGUAGE}   = $language       if $language;
    $bcp_arg{INTERFACES} = $interfacesFile if $interfacesFile;

    for (qw/BCP_OUT_P BCP_IN_S BCP_IN_U BCP_IN_P/) {
        no strict 'refs';
        $bcp_arg{$_} = $_;
        if ($OSNAME !~ /^MSWin/) {
            $bcp_arg{$_} =~ s/($_)/\$$1/;
        } else {
            $bcp_arg{$_} =~ s/($_)/%$1%/;
        }
    }
}

$Server = "SYBASE" if !$Server;

# Log us in to Sybase as '$User' and prompt for password if necessary.
#
# Note: The stty trick does not work on W32 systems.  Need some other means
#       of silently reading passwords.  In the mean time just don't try, it
#       is, afterall, DROS!

if (!$Password) {

    # Are we expecting the password from stdin or do we prompt?

    if ($stdin) {
        while (<>) {
            chomp;
            $Password = $_;
            last;
        }
    }
    else {

        print "Password: ";
        system("stty -echo")               if $OSNAME !~ /^MSWin/;

        $Password = <>;
        $Password = "" unless $Password;
        chomp($Password);

        system("stty echo")                if $OSNAME !~ /^MSWin/;
        print "\n"                         if $OSNAME !~ /^MSWin/;
    }
}

# They are allowed an alternative interfaces file if they so wish...

if($interfacesFile) {
    dbsetifile($interfacesFile);
}

# ...or character set...

if ($charset) {
    DBSETLCHARSET($charset);
}

# ...or locale...

if ($language) {
    DBSETLNATLANG($language);
}

# ...or packet size.  If they try to set this greater than the server's
# max, the script will fail.

if ($packet_size) {
  Sybase::DBlib::DBSETLPACKET($packet_size);
}

# Connect to Sybase and the set any database paramaters.

if (!($dbproc = new Sybase::DBlib ("$User",
                                   "$Password",
                                    $Server))) {
    die("Cannot connect to server.\n");
}

# We need a version string so that we can make some of the bits of the
# script optional.

$srvVersion = getSrvVersion();

# Just in case you compiled with dbNullIsUndef defaulting to FALSE

$dbproc->{"dbNullIsUndef"} = TRUE;

# Build the set of databases to extract.

if ($allUserDatabases) {

    $dbproc->dbcmd(qq{

        SELECT db.name
          FROM master.dbo.sysdatabases db
         WHERE db.name NOT IN (\'master\'     , \'sybsystemprocs\', \'tempdb\',
                               \'model\'      , \'sybsecurity\'   , \'sybsyntax\',
                               \'sybsystemdb\')
           AND db.name NOT LIKE \'\%RSSD\'

           });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        push(@databases, $dat[0]);
    }
}
elsif ($allDatabases) {

    $dbproc->dbcmd(qq{

        SELECT db.name
          FROM master.dbo.sysdatabases db
         WHERE db.name != \'tempdb\'  -- If you want this one, use -D and bear
                                      -- the consequences.

                                    });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        push(@databases, $dat[0]);
    }
}

# Remove a trailing ".sql" from the prefix if it is there.

if ($prefix_opt) {
    $prefix_opt =~ s/[.]sql$//;
}

# Remove unwanted databases.

foreach $ex_db (@exclude_db) {
    @databases = grep {$_ ne $ex_db} @databases;
}

# It would now be possible to have exactly 0 databases, so stop if that is
# the case.

die("No databases left to process after exclusions.\n") if 0 == @databases;

verifyDatabases(@databases);

if (@databases > 1) {
    $multi_db_extract = TRUE;
    buildDbDirs(@databases);
}

# If we the user specified -D there will be only one directory in the
# array, else there will be all of the databases to be dumped.

DATABASE:
foreach $database (@databases) {

    if ($multi_db_extract && !chdir($database)) {
        die("Cannot find the database subdirectory for $database.\n");
    }

    # Change the default prefix to be the database name if the split level
    # is 2 and the user has not specified one.

    if (!$prefix_opt && $splitLevel == 2) {
        $prefix = $database;
    }
    else
    {
        if ($prefix_opt && $prefix_opt =~ /[%]/) {
            $prefix = expand_prefix($prefix_opt, $Server, $database);
        }
        else {
            $prefix = (($prefix_opt) ? $prefix_opt : 'script');
        }
    }

    if ($firstDatabase) {
        $firstDatabase = 0;

        logMessageInit("Extraction started at:\t$date\n") if $verbose > 0;

        logMessage("Verbose logging selected.\n") if $verbose > 1;

        logOptions() if $verbose > 4;

        printSrvVersion();

    }

    # Initialise the output files (The.

    initScriptFile($database);

    # If we are splitting things into their atomic componets, ensure that
    # we have a set of directories that we can dump the files into.

    verifyDirectories if $splitLevel == 2;

    next DATABASE if !$dbproc->dbuse($database);

    if ($generateBCP) {
        # Set up the name for the bcp extraction.

        if ($multi_db_extract) {
            $bcp_out       = "$database.bcp_out.$shell_suffix";
            $bcp_in        = "$database.bcp_in.$shell_suffix";
        }
        else {
            $bcp_out       = "bcp_out.$shell_suffix";
            $bcp_in        = "bcp_in.$shell_suffix";
        }

        # If the output files are open, close them.  That way we will get a
        # different name for each databases bcp scripts.

        if (fileno(BCP_OUT)) {
            close(BCP_OUT) or die "Can't close $bcp_out: $!\n";
        }

        if (fileno(BCP_IN)) {
            close(BCP_IN) or die "Can't close $bcp_in: $!\n";
        }
    }

    # Clear out the hashes that hold the defaults.

    undef(%udflt);
    undef(%urule);

    logMessage("Running dbschema.pl($VERSION) on database \'$database\' on $date\n") if $verbose > 0;

##########################################################################
##
## Start extracting the objects.
##
## NOTE: The indexes and trigger extractions *must* be the last two in the
##       list.  Since the switchScript has no means of returning to the
##       previous script, they must be last.  If they are part of their own
##       scripts, then it does not matter if they are called last.  If they
##       are not to be extracted separately, then indexes are extracted as
##       part of the table script and triggers are just added to the bottom
##       and extractIndexes is never actually called.
##

    extractGroups($database)   if $outTypes =~ /g/;
    extractUsers($database)    if $outTypes =~ /u/;
    extractAliases($database)  if $outTypes =~ /a/;
    extractTypes($database)    if $outTypes =~ /y/;
    extractRules($database)    if $outTypes =~ /w/;
    extractDefaults($database) if $outTypes =~ /d/;
    extractBinds($database)    if $outTypes =~ /b/;
    extractTables($database)   if $outTypes =~ /t/;

    if ($splitLevel == 1) {
        extractFkeys($database) if $outTypes =~ /f/;
    }

    extractKeys($database)  if $outTypes =~ /k/;
    extractViews($database) if $outTypes =~ /v/;
    extractProcs($database) if $outTypes =~ /p/;
    extractTrigs($database) if $outTypes =~ /r/;

    if ($splitLevel == 1 || $separateIdxs) {
        extractIndexes($database) if $outTypes =~ /i/;
    }

    if ($extractSchema) {
        extractCreateDB($database);
        if (!$for_load) {
            extractSegments($database);
        }
    }

    if ($multi_db_extract && !chdir("..")) {
        die("Cannot return to base directory.\n");
    }
}

# Extract the schema, but only if so requested.  I have placed all of
# this together and at the end so that it can be easily hoiked out to
# allow you to create a server creation script.

if ($extractSchema) {
    extractLogins;
    extractDiskInit;
}

$date = scalar(localtime);

logMessage("\nExtraction finished at:\t$date\n") if $verbose > 0;
printStats()                                     if $verbose > 2;

logMessage("\nLooks like I'm all done!\n") if $verbose > 0;

close(SCRIPT);
close(LOG);
close(ERR);

dbexit;

#-------------------------#
# Subroutine definitions. #
#-------------------------#

sub checkPerms {
    my ($perm, $curperms) = @_;
    my ($newperms);

    $newperms = $curperms . "," . $perm;

    if ($newperms =~ /UPDATE/     &&
        $newperms =~ /SELECT/     &&
        $newperms =~ /REFERENCES/ &&
        $newperms =~ /INSERT/     &&
        $newperms =~ /DELETE/) {
        $newperms = "ALL";
    }

    return $newperms;
}

sub getPerms {
    my ($obj) = $_[0];
    my ($ret, @dat, $cnt);
    my ($action, $permsaffected, $cols, $user);

    $dbproc->dbcmd("exec sp_helprotect '$obj'\n");
    $dbproc->dbsqlexec;

    $cnt = 0;
    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while (@dat = $dbproc->dbnextrow) {

            if ($cnt == 0) {
                putScript("IF OBJECT_ID('$obj') IS NOT NULL\n");
                putScript("BEGIN\n");

                $action        = uc($dat[2]);
                $permsaffected = uc($dat[3]);
                $cols          = $dat[5];
                $user          = $dat[1];

                $cnt           = 1;

                next;
            }

            if (($action eq uc($dat[2])) &&
                ($cols   eq $dat[5])     &&
                ($user   eq $dat[1])) {

                $permsaffected = checkPerms(uc($dat[3]),$permsaffected);
            }
            else {
                putScript("    " . $action . " " .
                          $permsaffected . " ON $obj" .
                          (($cols =~ /^All$/) ? " " :
                           "(".$cols.") ") .
                          (($action =~ /Revoke/) ?
                           "FROM" : "TO") . " $user\n");

                $action        = uc($dat[2]);
                $permsaffected = uc($dat[3]);
                $cols          = $dat[5];
                $user          = $dat[1];
            }
        }
    }

    if ($cnt > 0) {
        putScript("    " . $action . " " . $permsaffected . " ON $obj" .
                  (($cols =~ /^All$/) ? " " : "(".$cols.") ") .
                  (($action =~ /Revoke/) ? "FROM" : "TO") . " $user\n");

        putScript("END\n");

        ++$Stats{'Permissions'};
    }

    $cnt;
}

sub getObj {
    my ($database, $objname, $obj, $objshortname) = @_;

    # 2002/03/12: mmertel added $number and $colid for stored procedure
    # grouping.

    my (@dat, @items, @vi, $found, $text, $number, $colid);

    my($statsKey);
    my($last_line);

    $statsKey = $objname . "s";

    # For procs we want to extract the independent ones first followed by
    # the dependent ones.  Will only work if the dependency is local.
    #
    # NOTE: What to do about those procs that rely on temp tables ie, a
    #       parent proc creates a temp table, calls a subordinate that
    #       fills it. When you build these by hand the temp table creation
    #       has to be there for the subordinate proc to build successfully.
    #       However, it is next to impossible to determine if this is the
    #       case when reverse engineering.  Probably the best I can do is
    #       issue a warning.  FIXME

    if ($obj eq "P" && !$for_compare) {
        $dbproc->dbcmd(qq{

            SELECT DISTINCT
                   o1.name,
                   u1.name,
                   o1.id
              FROM dbo.sysobjects    o1,
                   dbo.sysusers      u1,
                   dbo.sysprocedures p1
             WHERE o1.type           = \'$obj\'
               AND u1.uid            = o1.uid
               AND o1.id             = p1.id
               AND p1.status & 4096 != 4096
               AND o1.name LIKE \'$itemLike\'
               AND NOT EXISTS (SELECT 1
                                 FROM dbo.sysdepends d1,
                                      dbo.sysobjects o2
                                WHERE o1.id    = d1.id
                                  AND d1.depid = o2.id
                                  AND o2.type  = \'$obj\')
            UNION
            SELECT DISTINCT
                   o3.name,
                   u3.name,
                   o3.id
              FROM dbo.sysobjects    o3,
                   dbo.sysusers      u3,
                   dbo.sysprocedures p3
             WHERE o3.type           = \'$obj\'
               AND u3.uid            = o3.uid
               AND o3.id             = p3.id
               AND p3.status & 4096 != 4096
               AND o3.name LIKE \'$itemLike\'
               AND EXISTS (SELECT 1
                             FROM dbo.sysdepends d3,
                                  dbo.sysobjects o4
                            WHERE o3.id    = d3.id
                              AND d3.depid = o4.id
                              AND o4.type  = \'$obj\')

               });

    } else {
        $dbproc->dbcmd (qq{

            SELECT DISTINCT
                   o.name,
                   u.name,
                   o.id
              FROM dbo.sysobjects    o,
                   dbo.sysusers      u,
                   dbo.sysprocedures p
             WHERE o.type           = \'$obj\'
               AND u.uid            = o.uid
               AND o.id             = p.id
               AND p.status & 4096 != 4096
               AND o.name LIKE \'$itemLike\'
             ORDER BY o.name

             });
    }

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        push (@items, [ @dat ]);    # and save it in a list
    }

    # If there are no objects, and we are extracting each object into its
    # own file, then generate the dummy, empty script, anyway.
    #
    # I wish that I had written down exactly *why* I need to do this at
    # this point.  I am now looking at the code and wondering what I was
    # smoking :-)

    switchScript($database, "$objshortname", "empty") if $splitLevel == 2
                                                         and @items == 0;

    if (0 == @items && $comments) {
        putScript("/* No " . lc($objname) . "s found. */\n");
    }

    foreach (@items) {

        if (defined($Stats{$statsKey})) {
            ++$Stats{$statsKey};
        }

        @vi = @$_;
        $found = 0;

        logMessage("Extracting " . lc($objname) . " $vi[1].$vi[0]...") if $verbose > 1;

        # 00/10/17: tboss added $vi[1] to add the owner to the file name
        # generated by switchScript.  This is important, b/c you can have
        # two stored procs with the same name and different owners

        switchScript($database, "$objshortname", "$vi[1].$vi[0]") if $splitLevel == 2;

        putScript("/* $objname $vi[0], owner $vi[1] */\n") if $comments;

        if ($addDrops) {
            putScript("IF OBJECT_ID('$vi[1].$vi[0]') IS NOT NULL\n");
            putScript("BEGIN\n\n");

            # tboss 00/10/12: must setuser $vi[1] first, added owner $vi[1] to
            # drop object statement

            putScript("    setuser '$vi[1]'\n\n") if $setuser;

            putScript("    DROP " . uc($objname) . " $vi[1].$vi[0]\n\n");

            if ($verboseOutput) {
                putScript("    IF OBJECT_ID('$vi[1].$vi[0]') IS NOT NULL\n");
                putScript("        PRINT '<<< FAILED TO DROP " . uc($objname));
                putScript(" $vi[1].$vi[0] >>>'\n");
                putScript("    ELSE\n");
                putScript("        PRINT '<<< DROPPED " . uc($objname));
                putScript(" $vi[1].$vi[0] >>>'\n\n");
            }

            putScript("END\n");
            putScript("$cmdend\n");
        }

        # 00/10/12 tboss; added setuser $vi[1] command.

        putScript("\nsetuser '$vi[1]'\n$cmdend\n") if $setuser;

        # 2002/03/12: mmertel added number and colid to support stored
        # procedure grouping

        $dbproc->dbcmd(qq{

            SELECT number, colid, text
              FROM dbo.syscomments
             WHERE id = $vi[2]

                });

        $dbproc->dbsqlexec;
        $dbproc->dbresults;

        while(($number,$colid,$text) = $dbproc->dbnextrow) {
            # 03/12/2002: mmertel added a 'go' if this is the first line of
            # a new proc within a stored procedure group
            putScript("$cmdend\n") if $obj eq 'P' && $number > 1 and $colid == 1;

            putScript($text);

            $last_line = $text;
        }

        # Did the last $text include a carriage return, if not add one.
        # This seems a little pedantic, but if you have seen the way that
        # procs grow with some other extraction tools, you will understand
        # why.

        if (defined($last_line) && $last_line !~ /\n$/) {
            putScript("\n");
        }

        putScript("$cmdend\n\n");

        if ($verboseOutput) {
            putScript("IF OBJECT_ID('$vi[1].$vi[0]') IS NOT NULL\n");
            putScript("    PRINT '<<< CREATED " . uc($objname));
            putScript(" $vi[1].$vi[0] >>>'\n");
            putScript("ELSE\n");
            putScript("    PRINT '<<< FAILED TO CREATE " . uc($objname));
            putScript(" $vi[1].$vi[0] >>>'\n");
            putScript("$cmdend\n");
        }

        # 00/10/12; tboss; added the owner $vi[1] to the getPerms call
        if (($obj eq 'V' || $obj eq 'P') && $permissions) {
            getPerms("$vi[1].$vi[0]") && putScript("$cmdend\n");
        }

        logMessage("done.\n") if $verbose > 1;
    }
}

sub extractTables {
    my($database) = shift;
    my($ret);
    my(%tables);
    my($name);
    my(@tabnames);

    my($sql_text);
    my($has_identity);

    logMessage("Create Tables...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'tables') if $splitLevel == 1;

    # the fourth column (initialised to 'N') becomes the indicator that this
    # table has been printed (gets set to 'Y').

    $sql_text = qq{

        SELECT o.name,
               u.name,
               o.id,
               \'N\',
               0  -- Used to indicate precence of identity col
          FROM dbo.sysobjects o,
               dbo.sysusers   u
         WHERE o.name LIKE \'$itemLike\'
           AND o.type = \'U\'
           AND u.uid  = o.uid
         ORDER BY u.name, o.name

     };

    $dbproc->dbcmd($sql_text);

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@dat = $dbproc->dbnextrow) {
        $tables{$dat[1] . "." . $dat[0]} = [ @dat ];
        @tabnames = ( @tabnames, $dat[1] . "." . $dat[0] );
    }

    foreach $name (@tabnames) {
        dumpTable($database, \%tables, $tables{$name}, ());
        # 5th element of array indicates presence of identity.
        $has_identity = $tables{$name}->[4];
        if ($generateBCP) {
            printBcp($database, $name, $has_identity);
        }
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractIndexes {
    my($database) = shift;

    # Note: At the moment, this script is not perfect as an index rebuild
    # generator.  The reason is that if the table has a clustered index,
    # then there is no point in extracting all of the indexes since
    # rebuilding the clustered index will rebuild all of the non-clustered
    # indexes.  I think the best way to achieve this is to have two
    # methods: extractAllIndexes and extractClusteredIndexes, that are
    # called from here, and all this one does is choose which one to call.
    # Hmmm...  pause for thought.  Should also be a 'force' option so that
    # you can extract all of the indexes regardless.


    my(@col);

    my($table, $user, $uid, $fullname, @fields, $row, @rows);

    logMessage("Create Indexes...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    # Separate the indexes if splitLevel is 1 *or* "-f" was selected.
    switchScript($database, 'indexes') if ($splitLevel == 1 ||
                                           $separateIdxs);

    if ($comments && ($splitLevel < 2 || $separateIdxs)) {
        putScript("/* Indexes... */\n\n");
    }

    $dbproc->dbcmd(qq{

        SELECT o.name,
               u.name,
               o.id
          FROM dbo.sysobjects o,
               dbo.sysusers   u
         WHERE o.name LIKE \'$itemLike\'
           AND o.type    = \'U\'
           AND u.uid     = o.uid
         ORDER BY o.name

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@fields = $dbproc->dbnextrow) {
        push(@rows, [ @fields ]);
    }

    foreach $row (@rows) {

        ($table, $user, $uid) = @$row;

        $fullname = $user . "." . $table;

        @col = BuildIndexColumnsArray($fullname);

        dumpIndex($database, $table, $user, 'N', @col);
    }

    logMessage("done.\n") if $verbose > 0;

    switchScript($database, -1, -1)   # reopen the main script
}

sub extractFkeys {
    my($database) = shift;
    my (@refcol);

    logMessage("Add Foreign Key Constraints...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'fkeys') if $splitLevel == 1;

    if ($comments && $splitLevel < 2) {
        putScript("/* Foreign Key Constraints... */\n\n");
    }

    $dbproc->dbcmd(qq{

        SELECT isnull (r.frgndbname, \'$database\'),
               object_name (r.constrid),
               object_name (r.reftabid),
               -- object_name (r.reftabid, r.frgndbid),
               user_name (o2.uid),
               fokey1   = col_name (r.tableid,  r.fokey1),
               fokey2   = col_name (r.tableid,  r.fokey2),
               fokey3   = col_name (r.tableid,  r.fokey3),
               fokey4   = col_name (r.tableid,  r.fokey4),
               fokey5   = col_name (r.tableid,  r.fokey5),
               fokey6   = col_name (r.tableid,  r.fokey6),
               fokey7   = col_name (r.tableid,  r.fokey7),
               fokey8   = col_name (r.tableid,  r.fokey8),
               fokey9   = col_name (r.tableid,  r.fokey9),
               fokey10  = col_name (r.tableid,  r.fokey10),
               fokey11  = col_name (r.tableid,  r.fokey11),
               fokey12  = col_name (r.tableid,  r.fokey12),
               fokey13  = col_name (r.tableid,  r.fokey13),
               fokey14  = col_name (r.tableid,  r.fokey14),
               fokey15  = col_name (r.tableid,  r.fokey15),
               fokey16  = col_name (r.tableid,  r.fokey16),
               refkey1  = col_name (r.reftabid, r.refkey1),
               refkey2  = col_name (r.reftabid, r.refkey2),
               refkey3  = col_name (r.reftabid, r.refkey3),
               refkey4  = col_name (r.reftabid, r.refkey4),
               refkey5  = col_name (r.reftabid, r.refkey5),
               refkey6  = col_name (r.reftabid, r.refkey6),
               refkey7  = col_name (r.reftabid, r.refkey7),
               refkey8  = col_name (r.reftabid, r.refkey8),
               refkey9  = col_name (r.reftabid, r.refkey9),
               refkey10 = col_name (r.reftabid, r.refkey10),
               refkey11 = col_name (r.reftabid, r.refkey11),
               refkey12 = col_name (r.reftabid, r.refkey12),
               refkey13 = col_name (r.reftabid, r.refkey13),
               refkey14 = col_name (r.reftabid, r.refkey14),
               refkey15 = col_name (r.reftabid, r.refkey15),
               refkey16 = col_name (r.reftabid, r.refkey16),
               o1.name,
               user_name(o1.uid)
          FROM dbo.sysreferences r
              ,dbo.sysobjects    o1
              ,dbo.sysobjects    o2
         WHERE o1.name LIKE \'$itemLike\'
           AND o1.type = \'U\'
           AND r.pmrydbname IS NULL
           AND r.tableid   = o1.id
           AND r.reftabid *= o2.id
         ORDER BY 37, 38, 2

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    my (@refcols, $refname);
    my ($tabname, $tabname_prev) = ('', '');
    while ((@refcols = $dbproc->dbnextrow)) {

        ++$Stats{'References'};

        $tabname = $refcols[37] . '.' . $refcols[36];

        if ($tabname ne $tabname_prev) {
            if ($tabname_prev) {
                putScript("\nEND");
                putScript("\n$cmdend\n");
            }
            putScript("IF OBJECT_ID('$tabname') IS NOT NULL\n");
            putScript("BEGIN");
            $tabname_prev = $tabname;
        }

        $refname = $refcols[3] . "." . $refcols[2];

        if ($refcols[0] ne $database) {
          putScript("\n/* The following reference is in database\n");
          putScript("** $refcols[0], edit the script to create it manually\n*/\n");
          logError
            ($database,
             "Reference for $tabname in foreign database\n\t");
          $refname = $refcols[0] . "." . $refname;
        }
        putScript("\n");

        if ($addDrops) {
            putScript("    ALTER TABLE $tabname\n");
            putScript("        DROP CONSTRAINT $refcols[1]\n");
            putScript("\n");
        }


        putScript("    ALTER TABLE $tabname ADD\n");
        putScript("        CONSTRAINT $refcols[1]\n");
        putScript("            FOREIGN KEY (");

        printCols(@refcols[4..19]);

        putScript(")\n");
        putScript("            REFERENCES $refname (");

        printCols(@refcols[20..35]);

        putScript(")\n");

        if ($refcols[0] ne $database) {
            putScript("*/");
        }

    }

    if ($tabname) {
        putScript("\nEND");
        putScript("\n$cmdend\n");
    }

    logMessage("done.\n") if $verbose > 0;

}

sub extractBinds {
    my($database) = shift;
    my($dat, $dflt, $rule);

    my($putGo) = 0;

    logMessage("Bind rules & defaults to user data types...") if $verbose > 0;

    switchScript($database, 'binds') if $splitLevel == 1;

    putScript("/* Bind rules & defaults to user data types... */\n\n") if $comments;

    while (($dat, $dflt) = each(%udflt)) {
        ++$Stats{'Bind Defaults'};

        putScript("exec sp_bindefault $dflt, $dat\n");

        $putGo = 1;
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No defaults to bind. */\n\n") if $comments;
    }

    $putGo = 0;

    while (($dat, $rule) = each(%urule)) {
        ++$Stats{'Bind Rules'};

        putScript("exec sp_bindrule $rule, $dat\n");

        $putGo = 1;
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No rules to bind. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractDefaults {
    my($database) = shift;

    logMessage("Create defaults...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'defaults') if $splitLevel == 1;

    if ($comments && $splitLevel < 2) {
        putScript("\n/* Defaults... */\n\n");
    }

    getObj($database, 'Default', 'D', 'defaults');
    logMessage("done.\n") if $verbose > 0;

}

sub extractTypes {
    my($database) = shift;

    my($putGo) = 0;

    logMessage("Add user-defined data types...") if $verbose > 0;

    switchScript($database, 'types', -1) if $splitLevel > 0;

    putScript("/* Add user-defined data types: */\n\n") if $comments;

    $dbproc->dbcmd(qq{

        SELECT s.length,
               s.name,
               st.name,
               object_name(s.tdefault),
               object_name(s.domain),
               s.prec,
               s.scale,
               s.allownulls,
               isnull(s.ident, 1)
          FROM dbo.systypes s,
               dbo.systypes st
         WHERE st.type     = s.type
           AND s.usertype  > 100
           AND st.usertype < 100
           AND st.name NOT IN  (\'intn\', \'nvarchar\', \'sysname\', \'nchar\')

           });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;


    while ((@dat = $dbproc->dbnextrow)) {
        ++$Stats{'Types'};

        # FIXME: Should deal with the '...n' types as per table extraction.

        putScript("exec sp_addtype $dat[1], ");

        ($dat[2] =~ /char\b|binary\b/ and putScript("'$dat[2]($dat[0])'"))
            or
                ($dat[2] =~ /\b(numeric|decimal)\b/ and
                 putScript("'$dat[2]($dat[5],$dat[6])'"))
            or
                putScript("$dat[2]");

        # FIXME: IDENTITY and SQL Server.
        (($dat[8] == 1) and putScript(", 'IDENTITY'"))
            or
                (($dat[7] == 1) and putScript(", 'NULL'"))
            or
                putScript(", 'NOT NULL'");

        putScript("\n");

        # Now remember the default & rule for later.

        $urule{$dat[1]} = $dat[4] if defined($dat[4]);
        $udflt{$dat[1]} = $dat[3] if defined($dat[3]);

        $putGo = 1;
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No user defined types found. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractRules {
    my($database) = shift;

    logMessage("Create rules...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'rules') if $splitLevel == 1;

    if ($comments && $splitLevel < 2) {
        putScript("\n/* Rules... */\n\n");
    }

    getObj($database, 'Rule', 'R', 'rules');
    logMessage("done.\n") if $verbose > 0;
}

sub extractTrigs {
    my($database) = shift;

    logMessage("Create triggers...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'trigs') if ($splitLevel == 1 ||
                                         $separateTrigs);

    if ($comments && ($splitLevel < 2 || $separateTrigs)) {
        putScript("\n/* Triggers... */\n\n");
    }

    getObj($database, 'Trigger', 'TR', 'trigs');

    logMessage("done.\n") if $verbose > 0;

    switchScript($database, -1, -1)   # reopen the main script
}

sub extractProcs {
    my($database) = shift;

    logMessage("Create procedures...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'procs') if $splitLevel == 1;

    if ($comments && $splitLevel < 2) {
        putScript("\n/* Procedures... */\n\n");
    }

    getObj($database, 'Procedure', 'P', 'procs');

    logMessage("done.\n") if $verbose > 0;
}

sub extractViews {
    my($database) = shift;

    my(%views, @data, @viewnames, $view);

    logMessage("Create views...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    switchScript($database, 'views') if $splitLevel == 1;

    if ($comments && $splitLevel < 2) {
        putScript("\n/* Views... */\n\n");
    }

    if ($for_compare) {
        getObj($database, 'View', 'V', 'views');
    }
    else {

        $dbproc->dbcmd(qq{

            SELECT o.name,
                   u.name,
                   o.id,
                   'N'
              FROM dbo.sysobjects o,
                   dbo.sysusers   u
             WHERE o.name LIKE \'$itemLike\'
               AND o.type = \'V\'
               AND u.uid  = o.uid
             ORDER BY o.name

         });

        $dbproc->dbsqlexec;
        $dbproc->dbresults;

        while (@data = $dbproc->dbnextrow) {
            $views{$data[1] . "." . $data[0]} = [ @data ];

            push(@viewnames, $data[1] . "." . $data[0]);
        }

        foreach $view (@viewnames) {
            dumpView($database, \%views, $views{$view}, ());
        }

        if (0 == @viewnames) {
            putScript("/* No views found. */\n");
        }
    }
    logMessage("done.\n") if $verbose > 0;
}

sub extractKeys {
    my($database) = shift;

    logMessage("Create sp_*key definitions...") if $verbose > 0;
    logMessage("\n") if $verbose > 1;

    # Keys can all go in one file.
    switchScript($database, 'keys', -1) if $splitLevel > 0;

    putScript("\n/* Now create the key definitions ...*/\n\n") if $comments;

    # We must reset the user to dbo for the following, since only 'dbo'
    # seems to be able to add this information.

    putScript("\nsetuser 'dbo'\n$cmdend\n") if $setuser;

    $dbproc->dbcmd (qq{

        SELECT keytype        = convert(char(10), v.name),
               object         = object_name(k.id),
               related_object = object_name(k.depid),
               key1           = col_name(k.id, key1),
               key2           = col_name(k.id, key2),
               key3           = col_name(k.id, key3),
               key4           = col_name(k.id, key4),
               key5           = col_name(k.id, key5),
               key6           = col_name(k.id, key6),
               key7           = col_name(k.id, key7),
               key8           = col_name(k.id, key8),
               depkey1        = col_name(k.depid, depkey1),
               depkey2        = col_name(k.depid, depkey2),
               depkey3        = col_name(k.depid, depkey3),
               depkey4        = col_name(k.depid, depkey4),
               depkey5        = col_name(k.depid, depkey5),
               depkey6        = col_name(k.depid, depkey6),
               depkey7        = col_name(k.depid, depkey7),
               depkey8        = col_name(k.depid, depkey8)
          FROM dbo.syskeys k,
               master.dbo.spt_values v,
               dbo.sysobjects o
         WHERE k.type  = v.number
           AND v.type  = \'K\'
           AND k.id    = o.id
           AND o.type != \'S\'
           AND o.name LIKE \'$itemLike\'
         ORDER BY v.number, object, related_object

         });


    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {

      ++$Stats{'Keys'};

        if ($dat[0] eq "primary") {
            logMessage("Adding primary key $dat[1],...") if $verbose > 1;

            putScript("exec sp_primarykey $dat[1],");
            printCols(@dat[3..10]);
            putScript("\n$cmdend\n");
        }

        if ($dat[0] eq "foreign") {
            logMessage("Adding foreign key $dat[1],...") if $verbose > 1;

            putScript("exec sp_foreignkey $dat[1], $dat[2],");
            printCols(@dat[3..10]);
            putScript("\n$cmdend\n");
        }

        if ($dat[0] eq "common") {
          # sp_commonkey requires 2 tables and 2 columns.  The dep column
          # is in the second group, so we need to call printCols twice.
          #
          # Having discovered the bug, I am not 100% sure about the code.
          # It sort of depends (no pun intended) on key2 to key8 and
          # depkey2 to depkey8 being NULL.  I suspect that it needs a
          # little more thought.  FIX ME!

            logMessage("Adding common key $dat[1],...") if $verbose > 1;

            putScript("exec sp_commonkey $dat[1], $dat[2],");
            printCols(@dat[3..10]);
            putScript(", ");
            printCols(@dat[11..18]);
            putScript("\n$cmdend\n");
        }

        logMessage("done.\n") if $verbose > 1;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractGroups {
    my($database) = shift;

    my(@dat);
    my($ret);
    my($putGo) = 0;

    logMessage("Create groups...") if $verbose > 0;

    switchScript($database, 'groups', -1) if $splitLevel > 0;

    putScript("\n/* Groups... */\n\n") if $comments;

    # Ignore public and all roles.

    # Allow 'sa' to use the master table and regular users to simply access
    # local stuff.  Should work.  Do we actually need the first one?

    $dbproc->dbcmd(qq{

        IF (proc_role(\'sa_role\') = 1)
            SELECT name
              FROM sysusers su
             WHERE (uid > 16383 or uid = 0)
               AND name != \'public\'
               AND NOT EXISTS (SELECT 1
                                 FROM master.dbo.syssrvroles sr
                                WHERE sr.name = su.name)
        ELSE
            SELECT name
              FROM sysusers su
             WHERE (uid > 16383)
               AND name != \'public\'
               AND NOT EXISTS (SELECT 1
                                 FROM sysroles sr
                                WHERE sr.lrid = su.gid)
            });

    $dbproc->dbsqlexec;

    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while (@dat = $dbproc->dbnextrow) {
            ++$Stats{'Groups'};

            putScript("exec sp_addgroup '$dat[0]'\n");
            $putGo = 1;

            logMessage("Extracting group '$dat[0]'.\n") if $verbose > 1;
        }
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No groups found. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractUsers {
    my($database) = shift;


    my(@dat);
    my($putGo) = 0;

    logMessage("Create users...") if $verbose > 0;

    switchScript($database, 'users', -1) if $splitLevel > 0;

    putScript("\n/* Users... */\n\n") if $comments;

    # The outer join allows for the extraction of the guest user, if it has
    # been defined in a database.

    $dbproc->dbcmd(qq{

        SELECT
            sl.name,
            su.name,
            sg.name
        FROM
            sysusers su,
            sysusers sg,
            master.dbo.syslogins sl
        WHERE
            su.uid  <= 16383
        AND su.uid   > 1         -- Ignore 'dbo'
        AND su.suid *= sl.suid
        AND su.gid  *= sg.uid

        });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@dat = $dbproc->dbnextrow) {
      ++$Stats{'Users'};

      $dat[0] = $dat[0] || $dat[1];
      putScript("exec sp_adduser '$dat[0]', '$dat[1]'");
      putScript((($dat[2] eq 'public') ? "" : ", '$dat[2]'") . "\n");
      $putGo = 1;

      logMessage("Extracting user '$dat[1]' (login '$dat[0]')\n") if $verbose > 1;
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No users found. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractAliases {
    my($database) = shift;


    my(@dat);
    my($putGo) = 0;

    logMessage("Create aliases...") if $verbose > 0;

    switchScript($database, 'aliases', -1) if $splitLevel > 0;

    putScript("\n/* Aliases... */\n\n") if $comments;

    $dbproc->dbcmd(qq{

        SELECT sl.name,
               su.name
          FROM sysalternates sa,
               master.dbo.syslogins sl,
               sysusers su
         WHERE sa.suid    = sl.suid
           AND sa.altsuid = su.suid

           });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@dat = $dbproc->dbnextrow) {
      ++$Stats{'Aliases'};

      putScript("exec sp_addalias '$dat[0]', '$dat[1]'\n");
      $putGo = 1;

      logMessage("Extracting alias '$dat[0]' (aliased to '$dat[1]')\n") if $verbose > 1;
    }

    if ($putGo) {
        putScript("$cmdend\n\n");
    }
    else {
        putScript("/* No aliases found. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub extractLogins {

    my(@dat);
    my($putGo) = 0;

    logMessage("Create logins...") if $verbose > 0;

    switchScript("system", 'logins', -1) if $splitLevel  > 0;
    switchScript("system", 'serverDDL')  if $splitLevel == 0;

    putScript("\n/* Logins... */\n\n") if $comments;

    # Ignore public and all roles.

    $dbproc->dbcmd(qq{

        SELECT name,
               dbname
        FROM master.dbo.syslogins
        WHERE name NOT IN ('sa', 'probe')

                  });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@dat = $dbproc->dbnextrow) {

      ++$Stats{'Logins'};

      putScript("exec sp_addlogin '$dat[0]', 'DUMMYPASS', '$dat[1]'\n");
      $putGo = 1;
    }

    if ($putGo) {
        putScript("$cmdend\n");
    }
    else {
        putScript("/* No users found. */\n\n") if $comments;
    }

    logMessage("done.\n") if $verbose > 0;
}

sub rebuildConstraints {
# Coming soon!
}

sub rebuildIndexes {
# Coming soon!
}

sub getComment {
    my ($objid) = @_;

    my ($line, $text);

    $dbproc->dbcmd(qq{

        SELECT text
        FROM dbo.syscomments
        WHERE id = $objid

        });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    $text = "";

    while (($line) = $dbproc->dbnextrow) {
        $text = $text . $line if $line;
    }

    return $text;
}

sub printCols {

    my($col, $first);

    $first = 1;
    while ($col = shift(@_)) {
        last if ($col eq '*');
        putScript(", ") if !$first;
        $first = 0;
        putScript("$col");
    }
}

# We need a separate one of these for indexes because of the 'ASC', 'DESC'
# keywords, which have been added since version 11.9.

sub printIndexCols {
    my($col, $first, $order);

    $first = 1;
    while ($col = shift(@_)) {
        putScript(", ") if !$first;
        $first = 0;
        putScript("$col->[0]");

        if ($col->[1] ne "ASC") {
            putScript(" DESC");
        }
        elsif ($force_defaults) {  # It defaults to this, but might be wanted.
            putScript(" ASC");
        }
    }
}

# Note: this is a recursive subroutine.  If the current table references
# another that is in the list of tables to be dumped, and if that table has
# not yet been dumped, then dumpTable is called to dump it before
# proceeding.  If the table has already been dumped as part of a recursive
# call, then return immediately.  This recursion seems a little excessive,
# why not use crdate from sysobjects?  Sadly, that does not work.  If you
# create a table, call it slave1, and then create a second, call it
# master1, with a primary key and *then* link the slave to the master with
# a foreign key from a slave column referring to the master1 primary key.
# If you use crdate you will extract them as slave1 followed by master1.
# dumpTable does the correct thing of dumping master1 followed by slave1.
#
# Other orderings may be needed (alphabetical, date) so that database
# compares can happen.

sub dumpTable {

    # $tabref is a reference to the table being dumped, $tables_ref is a
    # reference to the set of tables in this database.

    my($database, $tables_ref, $tabref, @referers) = @_;

    return if $tabref->[3] eq "Y";

    my(@nul) = ('NOT NULL','    NULL');
    my(@dat, $dat, @col);
    my(@refcols, @reflist, @field, $rule, $dflt, %rule, %dflt);
    my($ddlrule, $ddldflt);
    my($refname, $first, $matchstring, $field, @constrids, $constrid);
    my($frgntabref, $nultype);
    my(@segment, $ret);
    my(@answer, @partitions, $partition);
    my(@answerlock, $rowlock, @expRowSize);
    my($sysstat2, $external_ref);

    my(@old_type) = (undef, 'tinyint', 'smallint', undef, 'integer');

    my($maxColLength, $maxTypLength, $thisTypLength);

    my(%tables) = %$tables_ref;

    my($fullname) = "$tabref->[1].$tabref->[0]";

    ++$Stats{'Tables'};

    # 4th element in $tables{$name} is 1 if the table has IDENTITY field
    # and 0 if it doesn't.
    $tabref->[4] = 0;

    # first, get any reference and ensure that dependent tables have already
    # been created

    # Add constraints that are dependent only upon the current database.
    # Inter-database dependencies will be added later as alter statements.

    # FIXME: This needs to be re-sorted in the same fashion as the dumpIndex
    # stuff.  Sybase (as of 12.0) allows more than 16 references (both ways)
    # and so it is poss to have more foreign and referential keys.

    if ($splitLevel != 2 && !$for_compare) {
        $dbproc->dbcmd (qq{

            SELECT isnull (r.frgndbname, \'$database\'),
                   object_name (r.constrid),
                   object_name (r.reftabid),
                   -- object_name (r.reftabid, r.frgndbid),
                   user_name (o2.uid),
                   fokey1   = col_name (r.tableid,  r.fokey1),
                   fokey2   = col_name (r.tableid,  r.fokey2),
                   fokey3   = col_name (r.tableid,  r.fokey3),
                   fokey4   = col_name (r.tableid,  r.fokey4),
                   fokey5   = col_name (r.tableid,  r.fokey5),
                   fokey6   = col_name (r.tableid,  r.fokey6),
                   fokey7   = col_name (r.tableid,  r.fokey7),
                   fokey8   = col_name (r.tableid,  r.fokey8),
                   fokey9   = col_name (r.tableid,  r.fokey9),
                   fokey10  = col_name (r.tableid,  r.fokey10),
                   fokey11  = col_name (r.tableid,  r.fokey11),
                   fokey12  = col_name (r.tableid,  r.fokey12),
                   fokey13  = col_name (r.tableid,  r.fokey13),
                   fokey14  = col_name (r.tableid,  r.fokey14),
                   fokey15  = col_name (r.tableid,  r.fokey15),
                   fokey16  = col_name (r.tableid,  r.fokey16),
                   refkey1  = col_name (r.reftabid, r.refkey1),
                   refkey2  = col_name (r.reftabid, r.refkey2),
                   refkey3  = col_name (r.reftabid, r.refkey3),
                   refkey4  = col_name (r.reftabid, r.refkey4),
                   refkey5  = col_name (r.reftabid, r.refkey5),
                   refkey6  = col_name (r.reftabid, r.refkey6),
                   refkey7  = col_name (r.reftabid, r.refkey7),
                   refkey8  = col_name (r.reftabid, r.refkey8),
                   refkey9  = col_name (r.reftabid, r.refkey9),
                   refkey10 = col_name (r.reftabid, r.refkey10),
                   refkey11 = col_name (r.reftabid, r.refkey11),
                   refkey12 = col_name (r.reftabid, r.refkey12),
                   refkey13 = col_name (r.reftabid, r.refkey13),
                   refkey14 = col_name (r.reftabid, r.refkey14),
                   refkey15 = col_name (r.reftabid, r.refkey15),
                   refkey16 = col_name (r.reftabid, r.refkey16)
              FROM dbo.sysreferences r
                  ,dbo.sysobjects    o1
                  ,dbo.sysobjects    o2
             WHERE r.pmrydbname IS NULL
               AND r.tableid   = o1.id
               AND o1.name     = \'$tabref->[0]\'
               AND o1.uid      = user_id(\'$tabref->[1]\')
               AND r.reftabid *= o2.id

               });

        $dbproc->dbsqlexec;
        $dbproc->dbresults;

        while ((@refcols = $dbproc->dbnextrow)) {
            push (@reflist, [ @refcols ]);
        }

        foreach (@reflist) {

            @refcols = @$_;

            # if the foreign table is in a foreign database or is not in
            # our table list, then don't do any more than add it to the list

            next if $refcols[0] ne $database;

            $refname = $refcols[3] . "." . $refcols[2];

            next if not defined ($tables{$refname});

            # ***TEST*** Skip circular links for same table

            next if $refname eq $fullname;

            $frgntabref = $tables{$refname};

            # otherwise check if it's already been dumped, if so, continue

            next if $frgntabref->[3] eq "Y";

            # make sure we aren't in a reference loop by checking to see if
            # this table is already in the heirarchy of refering tables
            # that led to the current invocation.

            if (grep {$_ eq $refname} @referers) {

                putScript("/* WARNING: circular foreign key ref to $refname */\n");
                logError($database, "$fullname in circular fkey ref to $refname\n");

                # I think that we could do a better job here, by adding a
                # couple more checks.  Not that hard really.  Best thought
                # at the moment is not to put references in the table but
                # have them all as alters.  Could have have an option.
                # That way, circular references could be avoided since the
                # table is already in existance. FIXME.
                return; # Don't bother to print any more, since we know it is circular
            }

            # so dump the referenced tables first

            dumpTable ($database, $tables_ref, $frgntabref, @referers, $refname);
        }
    }

    switchScript($database, "tables", "$fullname") if $splitLevel == 2;

    logMessage("Creating table $tabref->[0], owner $tabref->[1]\n") if $verbose > 1;

    putScript("\n/* Start of description of table $fullname */\n") if $comments;

    # 00/10/12 tboss; fixed to always put a setuser command

    putScript("\nsetuser '$tabref->[1]'\n$cmdend\n") if $setuser;

    # If we are trying to copy a database structure to a server, say a test
    # environment, using this script, dropping the table will drop the
    # indexes.

    if ($addDrops) {
        putScript("\nIF OBJECT_ID('$fullname') IS NOT NULL\n");
        putScript("BEGIN\n");
        putScript("    DROP TABLE $fullname\n");
    }

    if ($verboseOutput && $addDrops) {
        putScript("    IF OBJECT_ID('$fullname') IS NOT NULL\n");
        putScript("        PRINT '<<< FAILED TO DROP $fullname >>>'\n");
        putScript("    ELSE\n");
        putScript("        PRINT '<<< DROPPED TABLE $fullname >>>'\n");
    }

    if ($addDrops) {
        putScript("END\n");
        putScript("$cmdend\n");
    }

    $dbproc->dbcmd(qq{

        SELECT sysstat2
          FROM dbo.sysobjects
         WHERE name = \'$tabref->[0]\'

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@field = $dbproc->dbnextrow) {
        $sysstat2 = $field[0];
    }

    if (($sysstat2 & 1024) == 1024) { # CIS Table
        if ($cis) {  # Automatically false for SQL Server.
            $dbproc->dbcmd(qq{

                SELECT char_value
                  FROM sysattributes
                 WHERE class        = 9
                   AND attribute    = 1
                   AND object_cinfo = \'$tabref->[0]\'

                   });

            $dbproc->dbsqlexec;
            $dbproc->dbresults;

            while (@field = $dbproc->dbnextrow) {
                $external_ref = $field[0];
            }

            logError($database, "Found CIS table: $fullname -> $external_ref\n");

            putScript("\nexec sp_addobjectdef $tabref->[0], $external_ref, \'table\'\n");
            putScript("$cmdend\n\n");
        }
        else {
            logError($database, "Found CIS table: $fullname -> $external_ref.\n");
            logError($database, "Extracting as regular table since --nocis or --for-sql-server selected.\n");
        }
    }

    $dbproc->dbcmd(qq{

        SELECT DISTINCT
               Column_name  = c.name,
               Type         = t.name,
               Length       = c.length,
               Prec         = c.prec,
               Scale        = c.scale,
               Nulls        = convert(bit, (c.status & 8)),
               Default_name = object_name(c.cdefault),
               Rule_name    = object_name(c.domain),
               Ident        = convert(bit, (c.status & 0x80)),
               Default_Ddl  = isnull (d.status & 4096, 0),
               Rule_Ddl     = isnull (r.status & 4096, 0),
               DefaultId    = c.cdefault,
               RuleId       = c.domain,
               Column_len   = char_length(c.name),
               Type_len     = char_length(t.name)
          FROM dbo.syscolumns    c
              ,dbo.systypes      t
              ,dbo.sysprocedures d
              ,dbo.sysprocedures r
         WHERE c.id        = $tabref->[2]
           AND c.usertype *= t.usertype
           AND c.cdefault *= d.id
           AND c.domain   *= r.id
         ORDER BY c.colid

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    undef(%rule);
    undef(%dflt);

    putScript("\nCREATE ");

    if ($cis && (($sysstat2 & 1024) == 1024)) {
        putScript("EXISTING ");
    }

    putScript("TABLE $fullname (\n");

    $first = 1;
    @col   = ();

    $maxColLength = 0;
    $maxTypLength = 0;

    while (@field = $dbproc->dbnextrow) {

        if ($field[1] eq "intn") {
            $field[1] = $old_type[$field[2]];
            $field[5] = 1;
            logError($database,
                     "Found obsolete 'intn' datatype in $fullname.  " .
                     "Converting to $field[1].\n");
        }

        # The 'intn' version I am able to test using a system table.  The
        # following is untestable by me, so I will assume that someone will
        # let me know if it is wrong.

        if ($field[1] =~ /\b(datetimn|decimaln|floatn|moneyn|numericn)\b/) {
            $field[1] =~ s/n$//;
            $field[1] .= "e" if $field[1] eq "datetim";
            logError($database,
                     "Found obsolete '...n' datatype in $fullname.  " .
                     "Converting to $field[1].\n");
        }

        # convert uint, ubigint, usmallint, etc to unsigned int, unsigned bigint, etc
        print "field 1 = $field[1]\n";
        if ($field[1] =~ /\bu.*int.*\b/) {
          $field[1] = "unsigned ".substr($field[1],1); 
        }
        print "field 1 = $field[1]\n";

        push @col, [ @field ];

        $maxColLength = max($field[13], $maxColLength);

        $thisTypLength = $field[14];

        $thisTypLength += (length($field[2]) + 2)
            if $field[1] =~ /\b(char|varchar|nchar|nvarchar|binary|varbinary|sysname|float|unichar|univarchar)\b/;

        $thisTypLength += (length($field[3]) +
                           length($field[4]) + 3) if $field[1] =~ /\b(numeric|decimal)\b/;

        $maxTypLength = max($thisTypLength, $maxTypLength);
    }

    # There needs to be an option to allow people to upper case the types,
    # nullity and other bits.

    foreach (@col) {
        @field = @$_;

        # add a , and a \n if not first field in table
        putScript(",\n") if !$first;

        # get the declarative rule and default (if set)

        if ($field[9] != 0) {
            $ddldflt = getComment($field[11]);
        }
        else {
            $ddldflt = "";
        }

        if ($field[10] != 0) {
            $ddlrule = getComment($field[12]);
        }
        else {
            $ddlrule = "";
        }

        # Check if its an identity column
        if ($field[8] == 1) {
            # FIXME: Should we put out a comment, leave as it is or modify
            # to the IDENTITY(1,1) syntax for SQL Server.  Not sure.
            $nultype = "IDENTITY";

            # 4th element in $tables{$name} is 1 if the table has IDENTITY
            # field and 0 if it doesn't.
            $tabref->[4] = 1;
        }
        else {
            $nultype = $nul[$field[5]];
        }

        # If float, double the byte size to get the precision - COSTON
        if ($field[1] eq "float") {
            $field[2] = $field[2] * 2;
        }

        # Line things up for easier readibility.  We re-use the variable
        # for the length of the current type field, since it is probably
        # only a small cost to recalculate it.

        putScript("\t$field[0]" .
                  (" " x ($maxColLength + 1 - length($field[0]))) .
                  "$field[1]");

        $thisTypLength = length($field[1]);

        # Unichar and univarchar are only supported under UTF-8, but they
        # need to be here.
        #
        # NOTE: If we use extended regular expressions, then this will stop
        #       working with some versions of Perl.

        if ($field[1] =~
            /\b(char|varchar|nchar|nvarchar|binary|varbinary|sysname|float|unichar|univarchar)\b/) {
            putScript("($field[2])");
            $thisTypLength += (length($field[2]) + 2);
        }

        if ($field[1] =~ /\b(numeric|decimal)\b/) {
            putScript("($field[3],$field[4])");

            $thisTypLength += (length($field[3]) +
                               length($field[4]) + 3);
        }

        putScript((" " x ($maxTypLength - $thisTypLength)));

        putScript(" $ddldflt") if ($ddldflt && length($ddldflt) > 0);
        putScript(" $nultype") if ($nultype && length($nultype) > 0);
        putScript(" $ddlrule") if ($ddlrule && length($ddlrule) > 0);

        if (defined ($field[7])
            && ((!defined ($urule{$field[1]}))
                || $urule{$field[1]} ne $field[7])
            && ($field[10] == 0)) {
            $rule{"$tabref->[0].$field[0]"} = $field[7];
        }

        if (defined ($field[6])
            && ((!defined ($udflt{$field[1]}))
                || $udflt{$field[1]} ne $field[6])
            && ($field[9] == 0)) {
            $dflt{"$tabref->[0].$field[0]"} = $field[6];
        }

        $first = 0 if $first;
    }

    # references

    if ($splitLevel != 1 && $references) {
        foreach (@reflist) {

            @refcols = @$_;

            putScript(",");

            $refname = $refcols[3] . "." . $refcols[2];

            if ($refcols[0] ne $database) {
                putScript("\n/* The following reference is in database\n");
                putScript("** $refcols[0], edit the script to create it manually\n");

                logError($database,
                         "Reference for $fullname in foreign database\n\t");

                $refname = $refcols[0] . "." . $refname;
            }

            putScript("\n    ");

            $matchstring = substr($refcols[1], 0, 8) . "[_0-9][_0-9]*";
            $refcols[1] !~ /$matchstring/
                && putScript("CONSTRAINT $refcols[1] ");

            putScript("FOREIGN KEY (");

            printCols(@refcols[4..19]);

            putScript(") REFERENCES $refname (");

            printCols(@refcols[20..35]);

            putScript(")");

            if ($refcols[0] ne $database) {
                putScript("*/");
            }
        }

        @col = BuildIndexColumnsArray($fullname);

        foreach (@col) {

            @field = @$_;

            # if this is a key or unique constraint, print out the details
            # otherwise buffer it up to print as an index afterwards

            if ($field[3] & 2) {
                putScript(",\n    ");
                putScript("CONSTRAINT $field[0] ") unless ($field[3] & 8);

                if ($field[2] & 2048) {
                    putScript("PRIMARY KEY ");
                    if (($field[2] & 16) != 16 && ($field[3] & 512) != 512) {
                        putScript("NONCLUSTERED ") if ($field[1] != 1);
                    }
                }
                else {
                    putScript("UNIQUE ");
                    if (($field[2] & 16) == 16 || ($field[3] & 512) == 512) {
                        putScript("CLUSTERED ");
                    }
                }
                putScript("(");
                printIndexCols(@field[6..$#field]);
                putScript(")");
            }
        }
    }

    # Now do the table level check constraints

    @constrids = ();

    $dbproc->dbcmd(qq{

        SELECT constrid
          FROM dbo.sysconstraints
         WHERE tableid      = OBJECT_ID(\'$fullname\')
           AND status & 128 = 128
           AND colid        = 0

           });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (@field = $dbproc->dbnextrow) {
        @constrids = (@constrids, $field[0]);
    }

    foreach $constrid (@constrids) {
        putScript(",\n    " . getComment ($constrid));
    }

    putScript("\n)");

    # Don't bother with the locking scheme for SQL Server.

    if (!$for_sql_server) {

        # Print out any locking schemes information.  (This will be
        # backwards compatible since these bits are not set prior to
        # 11.9, so we will obtain the default result.)

        $dbproc->dbcmd (qq{

            -- Select the relevant bits.
            --
            --  0x2000  =  All Pages   (8192)
            --  0x4000  =  Data Pages (16384)
            --  0x8000  =  Data Rows  (32768)
            --
            --  0 => no explicit locking scheme, so it defaults to All Pages.

            SELECT sysstat2 & 57344
              FROM sysobjects
             WHERE id = OBJECT_ID(\'$fullname\')

                 });

        $dbproc->dbsqlexec;

        while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
            while (@answerlock = $dbproc->dbnextrow) {
                $rowlock = $answerlock[0];
                if ($rowlock == 16384) {
                    putScript("\nLOCK DATAPAGES");
                } elsif ($rowlock == 32768) {
                    putScript("\nLOCK DATAROWS");
                }
            }
        }

        # If the locking scheme is DOL (pages or rows), then add the
        # expected row size figure.

        if ($rowlock == 16384 ||
            $rowlock == 32768) {

            if ($splitLevel != 1) {
                $dbproc->dbcmd (qq{

                    SELECT exp_rowsize,
                           res_page_gap,
                           fill_factor,
                           maxrowsperpage
                      FROM sysindexes
                     WHERE id     = OBJECT_ID(\'$fullname\')
                       AND indid <= 1

                       });

                $dbproc->dbsqlexec;

                while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
                    while (@expRowSize = $dbproc->dbnextrow) {
                        if ($expRowSize[0] > 0) {
                            putScript("\nWITH EXP_ROW_SIZE = $expRowSize[0]");
                        }
                    }
                }
            }
        }
    }

    # Is the table built on a separate segment?

    @segment = undef;

    $dbproc->dbcmd (qq{

        IF EXISTS (SELECT 1
                     FROM sysobjects
                    WHERE id           = OBJECT_ID(\'$fullname\')
                      AND sysstat & 15 = 3)
        BEGIN
            SELECT s.name
              FROM sysobjects o,
                   syssegments s,
                   sysindexes i
             WHERE o.id      = OBJECT_ID(\'$fullname\')
               AND i.id      = o.id
               AND i.indid   < 2
               AND i.segment = s.segment
               AND s.name   != \'default\'
        END

        });

    $dbproc->dbsqlexec;

    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while (@segment = $dbproc->dbnextrow) {
            putScript("\nON $segment[0]");
        }
    }

    putScript("\n$cmdend\n\n");   # end of CREATE TABLE

    if ($verboseOutput) {
        putScript("IF OBJECT_ID('$fullname') IS NOT NULL\n");
        putScript("    PRINT '<<< CREATED TABLE $fullname >>>'\n");
        putScript("ELSE\n");
        putScript("    PRINT '<<< FAILED TO CREATE TABLE $fullname >>>'\n");
        putScript("$cmdend\n\n");
    }

    # Is the table partitioned?  This is feature is not available on
    # all releases of ASE, so check to see if syspartions exists
    # first.  Not available in this form at all in SQL Server.

    $dbproc->dbcmd (qq{

        IF EXISTS (SELECT 1
                     FROM sysobjects
                    WHERE type = \'S\'
                      AND name = \'syspartitions\')
            SELECT 'Y', \@\@version_as_integer
        ELSE
            SELECT 'N', \@\@version_as_integer

            });

    $dbproc->dbsqlexec;

    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while (@answer = $dbproc->dbnextrow) {
            $partition = $answer[0];
            $int_version = $answer[1];
        }
    }

    if ($partition) {
        # syspartitions table completely changed for ASE 15 & above
        if( $int_version < 15000 ) {
	    $dbproc->dbcmd (qq{

		SELECT COUNT(*)
		  FROM syspartitions
		 WHERE id = OBJECT_ID(\'$fullname\')

		 });
        } else {
            $dbproc->dbcmd (qq{

                SELECT COUNT(DISTINCT(partitionid))
                  FROM syspartitions
                 WHERE id = OBJECT_ID(\'$fullname\')

                 });
        }

        $dbproc->dbsqlexec;
        $dbproc->dbresults;

        while (@partitions = $dbproc->dbnextrow) {
            # Note, 1 partition is allowed in ASE 15, but not in ASE 12.5.x and lower
            if ($partitions[0] > 1) {
                if ($for_sql_server) {
                    logMessage
                        ("Warning: Table '$fullname' is partitioned, you are migrating ");
                    logMessage
                        ("to SQL Server so it will be ignored.\n");
                }
                else {
                    logMessage("\nTable is partitioned, ")            if $verbose > 2;
                    logMessage("extracting partition information...") if $verbose > 2;

                    putScript("IF OBJECT_ID('$fullname') IS NOT NULL\n");
                    putScript("    ALTER TABLE $fullname");
                    putScript(" PARTITION $partitions[0]\n");
                    putScript("$cmdend\n");

                    logMessage("done.\n") if $verbose > 2;
                }
            }
        }
    }

    # Only add the indexes to this script if the user has not specified
    # something else.

    if ($splitLevel != 1 && !$separateIdxs) {

        # If we are not dumping references, we will not have built the @col
        # array, so do it now.

        if (!$references) {
            @col = BuildIndexColumnsArray("$tabref->[1].$tabref->[0]");
        }

        dumpIndex($database, $tabref->[0], $tabref->[1], 'Y', @col);
    }

    logMessage("Add permission statements...\n")        if $permissions && $verbose > 2;
    putScript("\n/* Add permissions for table... */\n") if $permissions && $comments;

    if ($permissions) {
        getPerms("$fullname") && putScript("$cmdend\n\n");
    }

    logMessage("Bind rules & defaults to columns...\n")      if $verbose > 2;
    putScript("/* Bind rules & defaults to columns... */\n") if $comments;

    if($tabref->[1] ne 'dbo' && (keys(%dflt) || keys(%rule))) {
        putScript("/* The owner of the table is $tabref->[1].\n");
        putScript(" * Can't bind rules/defaults to tables I don\'t own.\n");
        putScript(" * The procs below will have to be run manualy");
        putScript(" * by user $tabref->[1].\n");
        putScript(" */\n");
        logError($database,
                 "Defaults/Rules for $fullname could not be bound\n");
    }

    while (($dat, $dflt)=each(%dflt)) {

        # This is wrong, but the fix is not trivial, or at least not quick.
        # This user can do what he likes to stuff he owns, but that does
        # not mean to say that he is 'dbo'.  For now, comment out all stuff
        # that is not owned by dbo to tell the users that there is
        # something that needs sorting.

        putScript("/* ") if $tabref->[1] ne 'dbo';
        putScript("exec sp_bindefault $dflt, '$dat'");

        if($tabref->[1] ne 'dbo') {
            putScript(" */\n");
        }
        else {
            putScript("\n$cmdend\n");
        }
    }

    while (($dat, $rule) = each(%rule)) {
        putScript("/* ") if $tabref->[1] ne 'dbo';
        putScript("exec sp_bindrule $rule, '$dat'");

        if($tabref->[1] ne 'dbo') {
            putScript(" */\n");
        }
        else {
            putScript("\n$cmdend\n");
        }
    }

    putScript("/* End of description of table $fullname */\n\n") if $comments;

    $tabref->[3] = "Y";
}

# This next could be used to dump procs as well.  However, it is not
# essential.  If it really irritates someone to get the error about
# sysdends, then alter this and send me the changes.

sub dumpView {
    my($database, $allViews_r, $view_r, @referers) = @_;

    return if $view_r->[3] eq "Y";

    my($last_line);
    my($fullname);

    $fullname = "$view_r->[1].$view_r->[0]";

    my(@reflist, @data, $view, $owner, $referenced_fullname, $referenced_view, $text);

    ++$Stats{'Views'};

    # Dump the dependent views first.  We might be able to generalise this later!

    if ($splitLevel != 2) {
        $dbproc->dbcmd (qq{

            SELECT  o.name,
                    u.name
              FROM  sysobjects o
                   ,sysdepends d
                   ,sysusers   u
             WHERE  o.id   = d.depid
               AND  d.id   = OBJECT_ID(\'$fullname\')
               AND  o.uid  = u.uid
               AND  o.type = \'V\'

               });

        $dbproc->dbsqlexec;
        $dbproc->dbresults;

        while (@data = $dbproc->dbnextrow) {
            push(@reflist, [ @data ]);
        }

        foreach (@reflist) {

            ($view, $owner) = @$_;

            $referenced_fullname = $owner . "." . $view;

            next if not defined($allViews_r->{$referenced_fullname});

            $referenced_view = $allViews_r->{$referenced_fullname};

            # Skip if the view has already been dumped.

            next if $referenced_view->[3] eq "Y";

            # Dump the referenced views first

            dumpView($database, $allViews_r, $referenced_view, @referers, $referenced_fullname);
        }
    }

    putScript("/* View $view_r->[0], owner $view_r->[1] */\n\n") if $comments;

    logMessage("Extracting view $fullname...") if $verbose > 1;

    if ($addDrops) {
        putScript("IF OBJECT_ID('$fullname') IS NOT NULL\n");
        putScript("BEGIN\n");
        putScript("    setuser '$view_r->[1]'\n") if $setuser;
        putScript("    DROP VIEW $fullname\n");

        if ($verboseOutput) {
            putScript("\n    IF OBJECT_ID('$fullname') IS NOT NULL\n");
            putScript("        PRINT '<<< FAILED TO DROP VIEW $fullname >>>'\n");
            putScript("    ELSE\n");
            putScript("        PRINT '<<< DROPPED VIEW $fullname >>>'\n\n");
            putScript("END\n");
        }

        putScript("$cmdend\n");
    }

    putScript("\nsetuser '$view_r->[1]'\n$cmdend\n") if $setuser;

    $dbproc->dbcmd(qq{

        SELECT text
          FROM dbo.syscomments
         WHERE id = OBJECT_ID(\'$fullname\')

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while (($text) = $dbproc->dbnextrow) {
        putScript($text);

        $last_line = $text;
    }

    if (defined($last_line) && $last_line !~ /\n$/) {
        putScript("\n");
    }

    putScript("$cmdend\n\n");

    if ($verboseOutput) {
        putScript("IF OBJECT_ID('$fullname') IS NOT NULL\n");
        putScript("    PRINT '<<< CREATED VIEW $fullname >>>'\n");
        putScript("ELSE\n");
        putScript("    PRINT '<<< FAILED TO CREATE VIEW $fullname >>>'\n");
        putScript("$cmdend\n\n");
    }

    if ($permissions) {
        # Using '&&' in the following part, means that the 'go' is only added
        # if there are perms to extract.

        getPerms("$fullname") && putScript("$cmdend\n");
    }

    $view_r->[3] = "Y";

    logMessage("done.\n") if $verbose > 1;
}

sub dumpIndex {
    my($database)     = shift;
    my($tablename)    = shift;
    my($owner)        = shift;
    my($just_indexes) = shift;
    my(@cols)         = @_;    # The rest of the arguments.

    my($fullname) = $owner . "." . $tablename;

    my (@field);

    logMessage("Extracting indexes for $fullname...\n") if $verbose > 2;

    foreach (@cols) {

        @field = @$_;

        ++$Stats{'Indexes'};

        logMessage("    $field[0]\n") if $verbose > 2;

        # if this is a key or unique constraint, print out the details
        # otherwise buffer it up to print as an index afterwards

        if ($field[3] & 2) {
            # Index supports primary key/unique constraint

            # If we are dumping the index as part of dumping a table, then
            # the constraints will already have been dealt with.  In that
            # case just ignore constraints and only dump out the explicit
            # indexes.

            if ($just_indexes eq 'N') {

                if ($addDrops) {
                    putScript("\nIF EXISTS (SELECT 1 FROM sysindexes\n");
                    putScript("              WHERE name = '$field[0]'\n");
                    putScript("                AND id   = OBJECT_ID('$fullname'))\n");
                    putScript("BEGIN\n");
                    putScript("    ALTER TABLE $fullname DROP CONSTRAINT $field[0]\n");

                    if ($verboseOutput) {
                        putScript("\n    IF EXISTS (SELECT 1\n");
                        putScript("                   FROM sysindexes\n");
                        putScript("                  WHERE id   = OBJECT_ID('$fullname')\n");
                        putScript("                    AND name = '$field[0]')\n");
                        putScript("        PRINT '<<< FAILED TO DROP CONSTRAINT $tablename.$field[0] >>>'\n");
                        putScript("    ELSE\n");
                        putScript("        PRINT '<<< DROPPED CONSTRAINT $tablename.$field[0] >>>'\n");
                    }

                    putScript("END\n");
                    putScript("$cmdend\n\n");
                }

                # Only do anything with tables that actually exist.
                putScript("IF OBJECT_ID('$fullname') IS NOT NULL\n");
                putScript("BEGIN\n");
                putScript("    ALTER TABLE $tablename ADD\n");
                putScript("    CONSTRAINT $field[0]\n") unless ($field[3] & 8);

                if ($field[2] & 2048) {
                    putScript("    PRIMARY KEY ");
                    if (($field[2] & 16) != 16 && ($field[3] & 512) != 512) {
                        putScript("NONCLUSTERED ");
                    }
                }
                else {
                    putScript("    UNIQUE ");
                    if (($field[2] & 16) == 16 || ($field[3] & 512) == 512) {
                        putScript("CLUSTERED ") if ($field[1] == 1);
                    }
                }

                putScript("(");
                printIndexCols(@field[6..$#field]);
                putScript(")\n");

                putScript("END\n");
                putScript("$cmdend\n\n");

                if ((($#field - 5) > 16) && $verbose > 1) {
                    logMessage("*** Warning ***: Source database has more than 16 columns in constraint.\n");
                    logMessage("                 Will not compile in versions < ASE 11.9\n");
                }

                if ($verboseOutput && $addDrops) {
                    putScript("\n        IF EXISTS (SELECT 1\n");
                    putScript("                     FROM sysindexes\n");
                    putScript("                    WHERE id   = OBJECT_ID('$fullname')\n");
                    putScript("                      AND name = '$field[0]')\n");
                    putScript("            PRINT '<<< CREATED CONSTRAINT $tablename.field[0] >>>'\n");
                    putScript("        ELSE\n");
                    putScript("            PRINT '<<< FAILED TO CREATE CONSTRAINT $tablename.field[0] >>>'\n");
                }
            }
        } else {
            # just an index

            if ($addDrops) {
                # Cannot specify owner when dropping and index, hence
                # $field[22] and not $tabname in the drop statement.
                putScript("\nIF EXISTS(SELECT 1 FROM sysindexes\n");
                putScript("           WHERE name = '$field[0]'\n");
                putScript("             AND id   = OBJECT_ID('$fullname'))\n");
                putScript("BEGIN\n");
                putScript("    DROP INDEX $tablename.$field[0]\n");

                if ($verboseOutput) {
                    putScript("\n    IF EXISTS (SELECT 1\n");
                    putScript("                   FROM sysindexes\n");
                    putScript("                  WHERE id   = OBJECT_ID('$fullname')\n");
                    putScript("                    AND name = '$field[0]')\n");
                    putScript("        PRINT '<<< FAILED TO DROP INDEX $tablename.$field[0] >>>'\n");
                    putScript("    ELSE\n");
                    putScript("        PRINT '<<< DROPPED INDEX $tablename.$field[0] >>>'\n");
                }

                putScript("END\n");
                putScript("$cmdend\n");
            }

            # Only do anything with tables that actually exist.
            putScript("\nIF OBJECT_ID('$fullname') IS NOT NULL\n");
            putScript("BEGIN\n");
            putScript("    CREATE ");

            putScript("UNIQUE ")          if $field[2] & 2;

            if ($srvVersion >= 11090000) {
                if (($field[2] & 16) == 16 || ($field[3] & 512) == 512) {
                    putScript("CLUSTERED ");
                } else {
                    putScript("NONCLUSTERED ");
                }
            } else {
                putScript("CLUSTERED ")       if $field[1] == 1;
                putScript("NONCLUSTERED ")    if $field[1] > 1;
            }

            putScript("INDEX $field[0]\n");
            putScript("    ON $tablename (");

            printIndexCols(@field[6..$#field]);

            putScript(")");

            # Print segment info if segment >= 3 (ie a segment other than default).
            # We do not need to put "on 'default'" since this *is* the default.

            putScript("\n    ON '$field[5]'") if ($field[4] >= 3);

            my $first = 1;
            if ($field[2] & 64) {
                putScript(" WITH ALLOW_DUP_ROW");
                $first = 0;
            }
            if ($field[2] & 1) {
                putScript((($first == 0) ? ", " : " WITH ") . "IGNORE_DUP_KEY");
                $first = 0;
            }
            if ($field[2] & 4) {
                putScript((($first == 0) ? ", " : " WITH ") . "IGNORE_DUP_ROW");
                $first = 0;
            }

            putScript("\nEND\n");
            putScript("$cmdend\n\n");

            if ((($#field - 5) > 16) && $verbose > 1) {
                logMessage("*** Warning ***: Source database has more than 16 columns in index.\n");
                logMessage("                 Will not compile in versions < ASE 12.0\n");
            }

            if ($verboseOutput) {
                putScript("\nIF EXISTS (SELECT 1\n");
                putScript("               FROM sysindexes\n");
                putScript("              WHERE id   = OBJECT_ID('$fullname')\n");
                putScript("                AND name = '$field[0]')\n");
                putScript("    PRINT '<<< CREATED INDEX $tablename.$field[0] >>>'\n");
                putScript("ELSE\n");
                putScript("    PRINT '<<< FAILED TO CREATE INDEX $tablename.$field[0] >>>'\n");
                putScript("$cmdend\n");
            }
        }
    }
}

sub BuildIndexColumnsArray {

    my($fullname) = shift;

    my(@col, @field, $field, @index, $i, $index);

    $dbproc->dbcmd (qq{

      SELECT sysind.name,
             sysind.indid,
             sysind.status,
             sysind.status2,
             sysind.segment,
             syeg.name
        FROM dbo.sysindexes sysind,
             dbo.syssegments syeg
       WHERE sysind.id      = OBJECT_ID(\'$fullname\')
         AND sysind.segment = syeg.segment
         AND sysind.indid BETWEEN 1 AND 254

         });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    @col = ();

    while (@field = $dbproc->dbnextrow) {
        push(@col, [ @field ]);
    }

  INDEX:
    foreach $index (@col) {
        $field = \@$index;

        $i = 1;
      COLUMN:
        while (1) {

            if ($srvVersion >= 11090000) {
                $dbproc->dbcmd (qq{

                    SELECT
                        index_col(\'$fullname\', $$field[1], $i),
                        index_colorder(\'$fullname\', $$field[1], $i)

                        });
            }
            else {
                $dbproc->dbcmd (qq{

                    SELECT
                        index_col(\'$fullname\', $$field[1], $i),
                        \'ASC\'

                        });
            }

            $dbproc->dbsqlexec;
            $dbproc->dbresults;

            while (@index = $dbproc->dbnextrow) {
                last COLUMN if !$index[0];
                push(@$field, [ @index ]);
            }

            #$dbproc->dbcancel;  # Ensure that there is no outstanding batch.

            $i++;
        }
    }

    return @col;
}

sub extractCreateDB {
    my($database) = shift;

    my($rowcount) = 0;
    my(@dat, $ret);

    my($first_log_dev) = 1;

    # We always want the DDL in a separate file, since it is of no use to
    # man nor beast in the main file.

    switchScript("system", 'create', -1 );

    logMessage("Producing database create statements ...") if $verbose > 0;

    putScript("\n/* Create DB commands... */\n\n") if $comments;

    $dbproc->dbcmd (qq{

        DECLARE \@numpgsmb  FLOAT       /* Number of Pages per Megabyte */

        SELECT \@numpgsmb = (1048576. / v.low)
          FROM master.dbo.spt_values v
         WHERE v.number = 1
           AND v.type   = \'E\'

        SELECT dv.name,
               su.segmap,
               CONVERT(INT,su.size / \@numpgsmb)
          FROM master.dbo.sysusages    su
              ,master.dbo.sysdatabases db
              ,master.dbo.sysdevices   dv
         WHERE su.dbid      = db.dbid
           AND db.name      = \'$database\'
           AND dv.cntrltype = 0
           AND su.vstart BETWEEN dv.low AND dv.high
         ORDER BY su.lstart

         });

    $dbproc->dbsqlexec;

    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while ((@dat = $dbproc->dbnextrow)) {
            ++$rowcount;

            if ($rowcount == 1) {
                putScript
                    ("CREATE DATABASE $database ON $dat[0] = $dat[2]");
            } else {
                putScript
                  ("ALTER DATABASE $database " .
                   (($dat[1] == 4) ? "LOG " : "") . "ON $dat[0] = $dat[2]");
            }

            if ($for_load) {
                putScript(" FOR LOAD");
            }

            putScript("\n$cmdend\n");

            if ($first_log_dev && $dat[1] == 4 && !$for_load) {

                # If we create a database in the normal manner, Sybase
                # makes the first device data only and the second log
                # only.  This does not work if you build one first then.
                # alter onto the second.  To compensate for this we add
                # the following command.

                putScript("\nexec sp_logdevice $database, $dat[0]\n$cmdend\n\n");
                $first_log_dev = 0;
            }
        }
    }
    logMessage("done.\n") if $verbose > 0;
}

# Extract the segments for a particular database.

sub extractSegments {
    my($database) = shift;

    my($rowcount) = 0;
    my(@dat, $ret);

    logMessage("Producing addsegment statements ...") if $verbose > 0;

    putScript("\n/* sp_addsegment commands... */\n\n") if $comments;


    if ($use_database) {

        # Don't bother switching files for segments, but we need to be in
        # the relevant database, so add a "use".

        if ($target_db) {
            putScript("use $target_db\n$cmdend\n");
        }
        else {
            putScript("use $database\n$cmdend\n");
        }
    }

    $dbproc->dbcmd (qq{

        CREATE TABLE \#spdbdesc
        (
          dbid     SMALLINT        NULL,
          dbdesc   VARCHAR(102)    NULL
        )

        DECLARE \@curdevice  VARCHAR(30),
                \@curseg     SMALLINT,
                \@segbit     INT

        SELECT \@curdevice = MIN(d.name)
          FROM master..sysusages u,
               master.dbo.sysdevices d
         WHERE u.dbid       = db_id()
           AND d.low       <= u.size + u.vstart
           AND d.high      >= u.size + u.vstart - 1
           AND d.status & 2 = 2

        WHILE (\@curdevice is not null)
        BEGIN
            /*
            ** We need an inner loop here to go through
            **  all the possible segment.
            */
            SELECT \@curseg = min(segment)
              FROM syssegments

            WHILE (\@curseg is not null)
            BEGIN
                IF (\@curseg < 31)
                    SELECT \@segbit = power(2, \@curseg)
                ELSE
                    SELECT \@segbit = low
                      FROM master.dbo.spt_values
                     WHERE type   = \'E\'
                       AND number = 2

                INSERT into \#spdbdesc
                SELECT \@curseg, \@curdevice
                  FROM master..sysusages     u,
                       master.dbo.sysdevices d,
                       master.dbo.spt_values v
                 WHERE u.segmap & \@segbit  = \@segbit
                   AND d.low               <= u.size + u.vstart
                   AND d.high              >= u.size + u.vstart - 1
                   AND u.dbid               = db_id()
                   AND d.status & 2         = 2
                   AND v.number             = 1
                   AND v.type               = \'E\'
                   AND d.name               = \@curdevice

                SELECT \@curseg = MIN(segment)
                  FROM syssegments
                 WHERE segment > \@curseg
            END

            SELECT \@curdevice = MIN(d.name)
              FROM master..sysusages     u,
                   master.dbo.sysdevices d
             WHERE u.dbid = db_id()
               AND d.low       <= u.size + u.vstart
               AND d.high      >= u.size + u.vstart - 1
               AND d.status & 2 = 2
               AND d.name       > \@curdevice
        END

        /*
        **  One last check for any devices that have no segments.
        */
        INSERT into \#spdbdesc
        SELECT null, d.name
          FROM master..sysusages     u,
               master.dbo.sysdevices d
         WHERE u.segmap     = 0
           AND d.low       <= u.size + u.vstart
           AND d.high      >= u.size + u.vstart - 1
           AND u.dbid       = db_id()
           AND d.status & 2 = 2

        SELECT dbdesc,
               name
          FROM \#spdbdesc,
               syssegments
         WHERE dbid = segment
           AND name not in (\'default\', \'system\', \'logsegment\')
               ORDER BY 1, 2

               });

    $dbproc->dbsqlexec;

    while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
        while ((@dat = $dbproc->dbnextrow)) {
            putScript("exec sp_addsegment $dat[1],$database,$dat[0]\n");
            putScript("$cmdend\n");
        }
    }

    logMessage("done.\n") if $verbose > 0;

    # Ensure that the temp table does not exist.

    $dbproc->dbcmd (qq{

        DROP TABLE \#spdbdesc

        });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;
    $dbproc->dbcancel;

}

# Get the device info and produce disk init statements - dap, 6/96

sub extractDiskInit {

    my($x, $devname, $physname, $devno, $ctrltype, $pages, $status);
    my(@dat);

    switchScript("system", 'diskinit', -1) if $splitLevel > 0;

    logMessage("Producing disk init statements ...") if $verbose > 0;

    putScript("\n/* Disk init commands... */\n\n") if $comments;

    $dbproc->dbcmd (qq{

        SELECT device_name   = v.name,
               physical_name = v.phyname,
               device_number = CONVERT(tinyint,
                                       SUBSTRING(CONVERT(BINARY(4), v.low),
                                                 a.low,
                                                 1)),
               cntrltype,
               pages         = (v.high - v.low + 1),
               status
          FROM master.dbo.sysdevices v,
               master.dbo.spt_values a
         WHERE a.type   = \'E\'
           AND a.number = 3
           AND v.name  != \'master\'
         ORDER BY v.low

           });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        $x = join('~', @dat);
        $x =~ s/ //g;
        ($devname, $physname, $devno, $ctrltype, $pages, $status) = split("~", $x);

        if ($ctrltype =~ /0/) {
            putScript("disk init\n");
            putScript("    name      = \'$devname\',\n");
            putScript("    physname  = \'$physname\',\n");
            putScript("    vdevno    = $devno,\n");
            putScript("    size      = $pages,\n");
            putScript("    cntrltype = $ctrltype");

            if ($srvVersion >= 12000000) {
                if ($status & 16384 == 16384 ||
                    $dsync          eq "on"  ||
                    $dsync          eq "true") {
                    putScript(",\n    dsync = \'true\'");
                }
                else {
                    putScript(",\n    dsync = \'false\'");
                }
            }

            putScript("\n$cmdend\n\n");
        }
    }
    logMessage("done.\n") if $verbose > 0;
}

sub switchScript {

    my($database)   = shift;

    my($scriptType) = shift;   # For -O1 this is used as the name of the file,
                               # in -O2 mode, used as the directory name.
    my($objectName) = shift;   # Optional parameter, only needed for -O2

    # a.m.
    my($dirs) = "procs|trigs|tables|indexes|fkeys|views|defaults|rules";
                                                           # Objects to be
                                                           # placed in their
                                                           # own dir.

    close(SCRIPT);

    # Note to self: The following is a little messy and could do with a bit
    # of a clean up.

    if ($splitLevel == 2) {
        if ((index($dirs, $scriptType) > -1) &&
            ($objectName ne "-1")) {
          open(SCRIPT, "> $scriptType/$prefix.$objectName.$suffix")
            or die "Can't open $scriptType/$prefix.$objectName.$suffix: $!\n";
        } elsif ($database eq "system") {
          open(SCRIPT, "> system-DDL/$scriptType.$suffix")
            or die "Can't open system-DDL/$scriptType.$suffix: $!\n";
        } else {
          open(SCRIPT, "> remainder/$prefix.$scriptType.$suffix")
            or die "Can't open remainder/$prefix.$scriptType.$suffix: $!\n";
        }
    } elsif ($scriptType ne "-1") {
        if ($prefix_opt) {
          open(SCRIPT, "> $prefix\_$scriptType.$suffix")
            or die "Can't open $prefix.$scriptType.$suffix: $!\n";
        } else {
          open(SCRIPT, "> $scriptType.$suffix")
            or die "Can't open $prefix.$scriptType.$suffix: $!\n";
        }
    }
      else {
        open(SCRIPT, ">> $prefix.$suffix")
            or die "Can't repoen $prefix.$suffix: $!\n";
        }

    if ($scriptType ne "-1") {
        putScript("/* Script generated by dbschema.pl($VERSION) on $date.  */\n") if $comments;

      if ($use_database) {
          if ($database eq "system") {
              putScript("\nuse master\n$cmdend\n");
          }
          elsif ($target_db) {
              putScript("\nuse $target_db\n$cmdend\n");
          }
          else {
              putScript("\nuse $database\n$cmdend\n");
          }
      }
  }
}

sub initScriptFile {
    my($database) = shift;

    close(SCRIPT);

    if ($splitLevel == 0) {
        open(SCRIPT, "> $prefix.$suffix")
            || die "Can't open $prefix.$suffix: $!\n";

        putScript("/* Script generated by dbschema.pl($VERSION) on $date.  */\n") if $comments;
        putScript("/* Script extracted on a $OSNAME system. */\n")                if $comments;

        if ($use_database) {
              if ($target_db) {
                  putScript("\nuse $target_db\n$cmdend\n");
              }
              else {
                  putScript("\nuse $database\n$cmdend\n");
              }
        }
    }

}

sub putScript {
    my($line) = shift(@_);

    # It is possible for some of the stored procedures to have completely
    # null lines, which causes this to puke.  If we are calling this with
    # nothing to write out, simply return.

    return if !defined($line);

    $line =~ s/\0//g;

    # If we are extracting to Unix, remove any ^Ms that may have been
    # added from developers building from MS DROS/W32.

    $line =~ s/\cM//g if $OSNAME !~ /^MSWin/;

    print SCRIPT $line;

}

sub logError {

    my($database) = shift;
    my($msg)      = shift;

    # Open and print to the error file upon the first error.
    if (!fileno(ERR)) {
        open(ERR, "> $prefix.err")
          || die "Can't open $prefix.err: $!\n";
        print ERR
          "Error log from dbschema.pl($VERSION) on DB $database on $date\n\n";
        print ERR
          "Following objects in '" . $prefix . ".sql' may be incorrectly generated.\n";
        print ERR
          "Please correct the script to remove any inconsistencies.\n\n";
    }

    print ERR "$database: $msg";
}

sub logMessageInit {

    my($msg)      = shift;

    # If we have asked for a verbose run, then open the log file first time
    # around.

    if (($verbose > 1) && !fileno(LOG)) {
        open(LOG, "> $prefix.log") or die "Can't open $prefix.log: $!\n";
        print LOG "Database extracted on a $OSNAME machine.\n";
    }

    print LOG $msg if $verbose > 1;
    print $msg;
    print "Starting to extract database $database at $date\n" if (($verbose > 1) && $database);
}

sub logMessage {
    my($msg)      = shift;

    # If the LOG device is not open, this is an error.

    if (($verbose > 1) && !fileno(LOG)) {
        die("The LOG device was not initialised.  Please pass this error onto dowen\@midsomer.org.\n");
    }

    print LOG $msg if $verbose > 1;
    print $msg;
}

sub printBcp {
    my($database, $tabname, $has_identity) = @_;

    my($bcp_string);

    if (!fileno(BCP_OUT)) {
        open(BCP_OUT, ">$bcp_out") or die "Can't open $bcp_out: $!\n";
    }

    if (!fileno(BCP_IN)) {
        open(BCP_IN, ">$bcp_in") or die "Can't open $bcp_in: $!\n";
    }

    $bcp_string  = buildBcpCmdLine("out", $database, $tabname);

    print BCP_OUT "$bcp_string\n";

    $bcp_string  = buildBcpCmdLine("in",  $database, $tabname);

    $bcp_string .= " -E" if $has_identity;

    print BCP_IN "$bcp_string\n";
}

sub showVersion {
    print STDERR "dbschema: $VERSION\n";
}

sub usage {

    print STDERR <<ENDUSAGE
Usage: dbschema.pl [-U username] [-P password] [-I interfaces_file] [-S server]
                   [-D database [-D database ...] | -a | -A] [-b] [-m] [-k]
                   [-o prefix] [-O level] [-i] [-r] [-V] [-t pattern] [-q] [-h]
                   [-T types] [--for-load] [--for-compare] [--[no]setuser]
                   [--stdin] [--[no]use-database] [--dsync={on|off|true|false}]
                   [--[no]comments] [--[no]permissions] [--[no]references]
                   [-F | --force-defaults] [-J | --charset charset] [--[no]cis]
                   [-z | --language language] [-c | --cmdend cmdend]
                   [--packet-size size] [--for-sql-server] [-v [-v ...]|--quiet]
                   [-X | --exclude-db database [-X | --exclude-db database]]
                   [--bcp-arg argument=value ...]
                   [--bcp-in-arg argument=value ...]
                   [--bcp-out-arg argument=value ...]

         -U       Sybase account to connect to server with (defaults to 'sa').
         -P       Password of above account (prompts if not supplied).
         -I       An alternative interfaces file
                    (defaults to \$SYBASE/interfaces on Unix and
                    \%SYBASE\%/ini/sql.ini on W32).
         -S       Server to connect to (defaults to \$DSQUERY or 'SYBASE')
         -D       Database(s) to be extracted.
         -J       Specify server charset.
         -z       Specify locale for client.
         -c       Use specified string as the command end in place of 'go'.
         -h       Print this help and exit.
         -q       Print version number and exit.
         -a       Select all user databases to be extracted (ie *not* master,
                  model, tempdb or sybsystemprocs and sybsecurity if it exists).
         -A       Select *all* databases to be extracted.  tempdb is not
                  extracted.  Use explicit -D to extract tempdb.
         -X       Explicitly exclude a database from extraction.
         -b       Generate bcp scripts for unloading/loading data.
         -t       Pattern to select files of that type.  'sp_%' for example
                  would bring back all objects with a name like 'sp_%'.  Pattern
                  must be suitable for inclusion in a standard 'like' clause.
         -V       Make the generated script(s) verbose.
         -v       Verbose generation mode, creates a log file.  The more '-v's
                  the more verbose.  Up to a max of 3 at the present time.
         -m       Generate server disk inits and database create statements.
         -k       Add drop statements for each object.
         -o       Specify the script file prefix for -O0 and -O1 modes.  Strips
                  a trailing '.sql' if it exists.  Defaults to 'script'.
                  Expands the string %S to name of the server and %D to the
                  name of the current database being extracted.  Whilst this
                  might seem a little redundant, some users collect all of their
                  DDL into one directory and are having to parse directory names
                  to get these names.
         -O       0  =>  Put all SQL into a single file.
                  1  =>  Split SQL into one file per object type (procs,
                         triggers, etc).
                  2  =>  Split SQL into one file per object and place all of the
                         files into directories below the current directory.
                         The default prefix is changed to be that of the
                         database name.
         -i       Forces the extraction of the indexes into a separate file,
                  regardless of extraction level set by the -O switch.
         -r       Forces the extraction of the triggers into a separate file,
                  regardless of extraction level set by the -O switch.
         -T       Specify which portions of output to produce.  Follow
                  with any combination of the following (default=all):
                    t  tables                  g  groups
                    i  indexes (indices?)      u  users
                    f  foreign keys            a  aliases
                    k  keys                    y  types
                    v  views                   w  rules
                    p  stored procedures       d  defaults
                    r  triggers                b  binds
                    x  nothing
                  'i' and 'f' work only with the -O1 option.  Use

                  dbschema.pl ... -Tx -m

                  to extract the database and device scripts only.

         --packet-size
                  Specify the packet size to connect with.
         --for-load
                  Adds 'for load' statements to the database creation script.
         --dsync={on|off|true|false}
                  Adds the dsync commands to 12.5 disk init commands.  Unlike
                  Sybase, you can use either 'on' or 'true', I will print the
                  appropriate string with the various commands.
         --for-compare
                  Extracts views and procedures in alphabetic order rather than
                  in an attempt to extract independent ones first followed by
                  dependent ones.
         --[no]cis
                  If the table is a CIS table, add a 'existing' clause in the
                  creation line as well as a 'sop_addobjectdef' statement.  If
                  --nocis is selected and a cis table is discovered a line is
                  added to the error file.  The table is still extracted, but
                  without the 'existing' clause.  --cis is the default.
         --[no]references
                  Enables the switching on and off the generation of foreign
                  key references for building tables.
         --[no]use-database
                  If the option is enabled, 'use <database>' statements are
                  added to output script, otherwise they are omitted.  The
                  default is to have them.
         --target-db
                  Useful if you are generating a script from a database with one
                  name to be used against a database with another name.  The
                  default is the name of the database being extracted from.
         --[no]comments
                  If the option is enabled, comment statements are added
                  to the output script, otherwise they are omitted.  The
                  default is to have comment statements.
         --[no]setuser
                  If the option is enabled, 'set user' statements are
                  added to the output script at appropriate points,
                  otherwise they are omitted.  The default is to have 'set
                  user' statements.
         --[no]permissions
                  If the option is enabled, permission statements are
                  added to the output script after tables, procedures or
                  views are created , otherwise they are omitted.  The
                  default is to have permission statements.
         --force-defaults
                  Used to force adding of default items that are optional
                  to the output file, ie 'ASC' in index creation or
                  "dsync='true'" in the disk init.
         --stdin
                  Allows that password to be piped in.  This will solve the
                  problem of the password appearing in ps(1) listings.  Use

                  echo your_password | dbschema.pl --stdin ...

         --for-sql-server
                  Helps with the generation of scripts to be used to build
                  databases on SQL Server.
         --exclude-db
                  Same as '-X'.
         --quiet
                  Runs the server in silent mode, so that nothing is printed
                  to stdout.  Overrides any amount of '-v's.
         --bcp-arg
                  Specify list of arguments passed to the bcp command line.
                  The arguments are a list of key=value pairs, where each key
                  is an argument to pass to bcp and the value is the to give
                  to the argument.  So

                  ... --bcp-arg t="|**|" ...

                  becomes

                  bcp ...  -t "|**|"
                  Bcp arguments that require no value should be passed the
                  empty string eg --bcp-arg c="".
         --bcp-in-arg
                  Same as --bcp-arg except this list of arguments is only
                  passed to bcp input direction (in) command line.
         --bcp-out-arg
                  Same as --bcp-arg except this list of arguments is only
                  passed to bcp output direction (out) command line.

ENDUSAGE
}

# Verify that there are a complete set of sub-directories for the "-O2" option.

sub verifyDirectories {
    # Procs

    my($dir);
    my(@dirs) = ("procs", "trigs", "tables",
                 "views", "defaults", "rules", "remainder");

    # If extracting schema into separate directories, then we need the
    # "system-DDL" directory.

    push(@dirs, "system-DDL") if $extractSchema;

    foreach $dir (@dirs) {
        die "$dir already exists as a plain file.\n" if -f $dir;
        if (!-d $dir) {
            # 493 (decimal) = 755 (octal)
            mkdir $dir, 493 || die "Cannot create $dir sub-directory\n";
        }
    }
}

sub verifyDatabases {
    my(@databases) = @_;

    my($database, $dbname, $ret);

    foreach $database (@databases) {

        $dbproc->dbcmd(qq{

            IF EXISTS (SELECT 1
                         FROM master.dbo.sysdatabases db
                        WHERE db.name = \'$database\')
                SELECT 1
            ELSE
                SELECT 0

             });

        $dbproc->dbsqlexec;

        while (($ret = $dbproc->dbresults) != NO_MORE_RESULTS && $ret != FAIL) {
            while (($dbname) = $dbproc->dbnextrow) {
                if (!$dbname) {
                    die("Cannot find database \'$database\' in catalog.\nAborting.\n");
                }
            }
        }
    }
}

sub buildDbDirs {
    # Procs

    my($dir);
    my(@dirs) = @_;

    push(@dirs, "system-DDL") if $extractSchema;

    foreach $dir (@dirs) {
        die "$dir already exists as a plain file.\n" if -f $dir;
        if (!-d $dir) {
            # 493 (decimal) = 755 (octal)
            mkdir $dir, 493 || die "Cannot create $dir sub-directory\n";
        }
    }
}

sub max {
  return ($_[0] > $_[1] ? $_[0] : $_[1]);
}

sub printStats {

  my($objType);

  logMessage("Extracted:\n");

  foreach $objType (keys %Stats) {
    logMessage("\t$Stats{$objType}\t\t$objType\n");
  }

  logMessage("(The stats only include numbers for explicit objects extracted.)\n");
}

sub getSrvVersion {

    my($versionString, @versionComponents);

    my(@IDcomponents, $majorID, $minorID, $subID, $subSubID);

    my($srvVersion);

    $dbproc->dbcmd (qq{

        SELECT \@\@version

        });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        $versionString = $dat[0];
    }

    # Now we need to parse the string returned from @@version, which is of
    # the form:
    #
    # Adaptive Server Enterprise/12.5/B/Linux Intel/Linux 2.2.14-5.0smp ...
    #
    # Obviously we are interested in the second component.  We need to be
    # careful, since Sybase has released everything from 4.9 to 4.9.2 to
    # 11.9 to 11 to 11.0.3.3.  I think that we will map as follows:
    #
    # 11        -->     11000000
    # 11.0.1    -->     11000100
    # 11.0.3.3  -->     11000303
    # 11.9.2    -->     11090200
    # 12.0      -->     12000000
    # 12.5      -->     12050000
    #
    # Which should give us a monotonic increasing sequence of numbers.  If
    # they ever go to 11.0.3.3.1 I will formally hand in my notice as
    # maintainer of this script.

    return 0 if !defined($versionString);

    @versionComponents = split(/\//, $versionString);

    return 0 if 0 == @versionComponents;

    @IDcomponents = split(/\./, $versionComponents[1]);

    return 0 if 0 == @IDcomponents;

    # The version can be of the form SQL Server/11.0.3.3 ESD#6/..., so we
    # need to remove any non-numeric parts to the last element of the
    # array.

    $IDcomponents[$#IDcomponents] =~ s/\D.+$//g;

    $srvVersion  = shift(@IDcomponents) * 1000000;

    if (defined($IDcomponents[0])) {
        $srvVersion += shift(@IDcomponents) * 10000;
    }

    if (defined($IDcomponents[0])) {
        $srvVersion += shift(@IDcomponents) * 100;
    }

    if (defined($IDcomponents[0])) {
        $srvVersion += shift(@IDcomponents);
    }

    return $srvVersion;
}

sub printSrvVersion {

    my($versionString, @versionComponents);

    my(@IDcomponents, $majorID, $minorID, $subID, $subSubID);

    my($srvVersion);

    $dbproc->dbcmd (qq{

        SELECT \@\@version

        });

    $dbproc->dbsqlexec;
    $dbproc->dbresults;

    while ((@dat = $dbproc->dbnextrow)) {
        $versionString = $dat[0];
    }

    return if !defined($versionString);

    @versionComponents = split(/\//, $versionString);

    return 0 if 0 == @versionComponents;

    logMessage("ASE Version " . $versionComponents[1] . "\n") if $verbose > 0;
}

sub expand_prefix {
    my($prefix)   = shift;
    my($server)   = shift;
    my($database) = shift;

    my($is_pct)     = 0;
    my($char);
    my($out_string) = "";
    my($i);

    for ($i = 0; $i < length($prefix); $i++) {
        $char = substr($prefix, $i, 1);

        if ($char eq "%") {
            if ($is_pct) {
                $out_string .= "%";
                $is_pct = 0;
            }
            else {
                $is_pct = 1;
            }
        }
        elsif ($is_pct) {
            if ($char =~ /s/i) {
                $out_string .= $server;
            }
            elsif ($char =~ /d/i) {
                $out_string .= $database;
            }
            else {
                $out_string .= "%$char";
            }
            $is_pct = 0;
        }
        else {
            $out_string .= $char;
        }
    }
    return $out_string;
}

sub whoami {

    my($id);

    if ($OSNAME =~ /^MSWin/) {
        $id = 'sa';
    }
    else {
        $id = `id`;
        $id =~ s/^uid=[0-9]*\(([^)]+)\).*/$1/;
    }

    return $id;
}

sub logOptions {

    my($i);

    logMessage("\n\n");
    logMessage("Version:            $VERSION\n");
    logMessage("User:               $User\n");
    logMessage("Server:             $Server\n");

    for ($i=0; $i<=$#databases; $i++) {
        if ($i == 0) {
            logMessage("Database" . (@databases > 1 ? "s:" : ": ") . "           $database\n");
        }
        else {
            logMessage("                     $database\n");
        }
    }

    logMessage("Interfaces file:     $interfacesFile\n");
    logMessage("Command end:         $cmdend\n");
    logMessage("Suffix:              $suffix\n");
    logMessage("Prefix:              $prefix\n");
    logMessage("Item selection:      $itemLike\n");
    logMessage("Split level:         $splitLevel\n");
    logMessage("Chatacter set:       $charset\n")     if $charset;
    logMessage("Local language:      $language\n")    if $language;
    logMessage("Packet size;         $packet_size\n") if $packet_size;
    logMessage("Objects selected:    $outTypes\n");
    logMessage("CIS:                 " . ($cis            ? "on" : "off") . "\n");
    logMessage("Use database:        " . ($use_database   ? "on" : "off") . "\n");
    logMessage("Comments:            " . ($comments       ? "on" : "off") . "\n");
    logMessage("Setuser:             " . ($setuser        ? "on" : "off") . "\n");
    logMessage("Permissions:         " . ($permissions    ? "on" : "off") . "\n");
    logMessage("For load:            " . ($for_load       ? "on" : "off") . "\n");
    logMessage("Force defaults:      " . ($force_defaults ? "on" : "off") . "\n");
    logMessage("Dsync:               " . ($dsync          ? "on" : "off") . "\n");
    logMessage("Verbose output:      " . ($verboseOutput  ? "on" : "off") . "\n");
    logMessage("Extract Schema:      " . ($extractSchema  ? "on" : "off") . "\n");
    logMessage("Add drop statements: " . ($addDrops       ? "on" : "off") . "\n");
    logMessage("Generate BCP files:  " . ($generateBCP    ? "on" : "off") . "\n");
    logMessage("Separate Indexes:    " . ($separateIdxs   ? "on" : "off") . "\n");
    logMessage("Separate Triggers:   " . ($separateTrigs  ? "on" : "off") . "\n");
    logMessage("ASA:                 " . ($for_asa        ? "on" : "off") . "\n");
    logMessage("SQL Server:          " . ($for_sql_server ? "on" : "off") . "\n");
    logMessage("Index rebuild:       " . ($index_rebuild  ? "on" : "off") . "\n");


}

sub buildBcpCmdLine {
    my($direction, $database, $tabname) = @_;

    my($direction_args);     # bcp parameters specific for copy direction
    my($ext);
    my($k, $v);
    my($bcp_s, $bcp_u, $bcp_p);
    my($cl);

    if ($direction eq "out") {

        $direction_args = \%bcpOutArguments;

        $bcp_s = "BCP_OUT_S";
        $bcp_u = "BCP_OUT_U";
        $bcp_p = "BCP_OUT_P";
    } elsif ($direction eq "in"){

        $direction_args = \%bcpInArguments;

        $bcp_s = "BCP_IN_S";
        $bcp_u = "BCP_IN_U";
        $bcp_p = "BCP_IN_P";
    } else {
        die "buildBcpCmdLine: copy direction must be 'in' or 'out'";
    }

    # bcp file extension
    $ext = (exists $direction_args->{c}) ? "txt" :
           (exists $direction_args->{n}) ? "dat" :
           (exists $bcpArguments{n})     ? "dat" : "txt";

    # bcp command line
    $cl  = "bcp $database.$tabname " . $direction . " ${tabname}." . $ext;
    $cl .= " -S$bcp_arg{$bcp_s} -U$bcp_arg{$bcp_u} -P$bcp_arg{$bcp_p}";

    $cl .= " -J $bcp_arg{CHARSET}"    if $bcp_arg{CHARSET};
    $cl .= " -z $bcp_arg{LANGUAGE}"   if $bcp_arg{LANGUAGE};
    $cl .= " -I $bcp_arg{INTERFACES}" if $bcp_arg{INTERFACES};

    if ((scalar(keys(%bcpArguments)) + scalar(keys(%$direction_args))) != 0) {
        # add bcp parameters
        foreach my $h (\%bcpArguments, $direction_args) {
            while (($k, $v) = each %$h) {
                $cl .= " -$k";
                # add parameter value, if exists
                $cl .= " $v" if $v ne '""' && $v ne "''";
            }
        }
    }
    else {
        # For consistent backward behaviour, add -c if they have specified
        # no options.
        $cl .= " -c";
    }

    return $cl;
}

=head1 NAME

dbschema - a script to extract the schema from a Sybase ASE database.

=head1 README

dbschema.pl is a Perl script that will extract a database structure from
a Sybase ASE database utilising the Sybase::DBlib module.

=head1 DESCRIPTION

Whilst schema extraction is what this is script is all about, it can do
that one function in probably as flexible a manner as you could wish.

It can extract a single database into a single file, a single file per
object type (tables, procs, indexes etc) or a single file per actual
object.  The number of options is getting so large that I am writing a Tk
GUI to make it easier to manage, as well as the ability to extract single
objects in a point and shoot fashion!

To install Sybase::DBlib, you need to install the Sybperl package that
can be obtained from CPAN or directly from Michael Peppler's site:

http://www.mbay.net/~mpeppler

There is an FAQ and a mailing list for Sybperl, details of which can be
found on Michael's site.

There is nothing to actually install in order to get dbschema.pl to work.

Home:         The latest release of dbschema.pl can be obtained from
              http://www.midsomer.org

Maintainer:   David Owen (dowen@midsomer.org)

Anyone interested in contributing, please get in touch.  If you find this
script useful, then I would be grateful if you would send me details of your
locality throughout the world so that I can add you to my xearth file.

Thanks

dowen

=head1 PREREQUISITES

This script reuires the Sybase::DBlib module.

=head1 COREQUISITES

None.

=pod OSNAMES

Any that support Sybase::DBlib.

=pod SCRIPT CATEGORIES

Databases/Sybase

=cut

### Local Variables: ***
### perl-indent-level: 4 ***
### End: ***

