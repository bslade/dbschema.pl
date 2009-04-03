#!/usr/bin/perl -w
#
#   $Id: migrate_to_sqlserver.pl,v 1.1 2003/01/02 04:59:15 dowen Exp $
#
#   FIXME.

# Not sure what of these we will need.

use strict;
#use Sybase::DBlib;
use Getopt::Long;
use English;

# Local variables.

my($print_count) = 0;  # The nth print variable indicator.
my($init_spaces);
my($start_of_string);
my($subst_param_string);
my(@subst_params);
my(@line_parts);
my($save_string);
my($param);
my($dummy_variable);
my($i);
my($out_string);
my($format_string);

# Add stuff to here to post process files with (triggers and procedures) to
# get as much of a converstion to M$ as possible.  I know, it is a hateful
# thing that I have to do, and I will be writing the 'migrate_to_ase' and
# 'migrate_to_asa' as soon as I can.  However, a client is paying for this,
# so I have to do it.  Sorry if it upsets you, or if you think that I have
# gone to the dark side.

while (<>) {

    # Sybase allows strings to be delimited by either " or ', M$ does not.
    # Indiscriminent replacement leads to trouble, such that the legal
    # string <'..."...'> becomes the illegal string <'...'...'>.  We can
    # check to see if we are in the middle of a string before doing the
    # replace.

    if (/\"/) { # we have a double quote, so let's safe quote it.
        $_ = safe_quote($_);
    }

    # The 'char_length' function is deprecated.  The function is a drop in
    # replacement, so no marker is necessary.

    s/char_length/datalength/ig;

    # proc_role does not exist, have to use is_member.  This is not a
    # perfect fit, but better than nothing.  Might choose to put out a
    # warning.

    s/proc_role/is_member/ig;

    # Optimizer hints are not allowed in the way Sybase specifies them.  I
    # want to be able to do something special here, but not sure what yet.

    s/(\(index .*?\))/\/\*$1\*\//ig;  # (index ind_1) ==> /*(index ind_1)*/

    # Cursors are troublesome.  @@sqlstatus becomes @@FETCH_STATUS, but it
    # is not that simple.  They have different result codes.  Needs more
    # work!  Add a marker to the end of the line in such a case to come
    # back and fix up the code, since there is no real match for the return
    # values.

    if (/\@\@sqlstatus/i) { # We do this so that we can flag the code as
                            # well as replace it.
        s/\@\@sqlstatus/\@\@FETCH_STATUS/ig;
        s/$/ -- FIXME/;
    }

    # Cursor deallocation is different too, but much easier.
    # DEALLOCATE CURSOR <name> ==> DEALLOCATE <name>

    s/DEALLOCATE\s+CURSOR/DEALLOCATE/ig;

    # ROLLBACK TRIGGER is not to be found in SQL Server either, so replace
    # with a ROLLBACK TRAN. and a flag.

    if (/ROLLBACK\s+TRIGGER/i) {
        s/ROLLBACK\s+TRIGGER/ROLLBACK TRANSACTION/ig;

        if (/\bwith\b/i) {
            s/\bwith\b/-- with/ig;
        }

        s/$/ -- FIXME/;
    }

    # The Sybase raiserror is another example of Syntax gone barmy.  As a
    # compiler writer, it makes me *MAD* to see the bloody awful botches
    # that are allowed in to the language.  How can any sane person allow
    # it?  We have a lot of different types of raiserror, so deal with them
    # one at a time.  We use a generic 16, 1 for severity and state.

    # First ensure that we are not to the right of a comment indicator.
    # Just the '--' variety.  We are not trying to parse the whole file,
    # just a line at a time.  If the code has some awful combination of
    # raiserror -- raiserror, I am not going to *try* catch it!

    if (/raiserror/i && !/\-\-.*raiserror/i) {

        # 1. raiserror <number> <string>

        if (/raiserror\s+[0-9]+\s+'.+'/i) {
            s/raiserror\s+([0-9]+)\s+('.+')/RAISERROR($2, 16, 1)  -- FIXME errno was: $1/i;
        }

        # 2. raiserror <number> @<variable>

        elsif (/raiserror\s+[0-9]+\s+\@\S+/i) {
            s/raiserror\s+([0-9]+)\s+(\@\S+)/RAISERROR($2, 16, 1)  -- FIXME errno was: $1/i;
        }

        # 3. raiserror <number>, <string>

        elsif (/raiserror\s+[0-9]+\s*,\s*'.+'/i) {
            s/raiserror\s+([0-9]+)\s*,\s*('.+')/RAISERROR($1, 16, 1)  -- FIXME subst string was: $2/i;
        }

        # 4. raiserror @<variable>

        elsif (/raiserror\s+\@\S+/i) {
            s/raiserror\s+(\@\S+)/RAISERROR($1, 16, 1)  -- FIXME/i;
        }

        # 5. raiserror <number>

        elsif (/raiserror\s+[0-9]+/i) {
            s/raiserror\s+([0-9]+)/RAISERROR($1, 16, 1)  -- FIXME/i;
        }
    }

    # Some 'set' statements don't have an equivalent in SQL Server, comment
    # those out.

    s/set\s+table\s+count(.*)$/--set table count $1 -- FIXME/i;

    # Print statements within SQL Server do not take variables, so declare
    # a local variable, build the string and then print the variable.  We
    # only need to change those with variable substitutuion.

    if (/\%[1-9][0-9]*\!/i) {

        $save_string = $_;

        # We need the subst parameters.  They were in the form:
        #
        #   ... print "string", stuff, stuff, stuff...

        s/(.*)(print\s+)(\'.*?\')(\s*?,)(.*)$/$1$2$3$4$5/i;
        $init_spaces        = $1;
        $format_string      = $3;
        $start_of_string    = $4;
        $subst_param_string = $5;

        @subst_params = split /\s*,\s*/, $subst_param_string;

        for ($i = 0; $i <= $#subst_params; $i++) {
            # Remove any reading white space and single quote, if it exists.
            $subst_params[$i] =~ s/^\s+(\S)/$1/;
            $subst_params[$i] =~ s/(\S)\s+$/$1/;
            $subst_params[$i] =~ s/^\'//;
            $subst_params[$i] =~ s/\'$//;
        }

        @line_parts = split /\%[1-9][0-9]*\!/, $format_string;

        $out_string = '';

        for ($i = 0; $i <= $#line_parts; $i++) {
            $out_string .= $line_parts[$i];

            if (defined($subst_params[$i])) {
                $out_string .= ("' + " . $subst_params[$i] . " + '");
            }
        }

        $dummy_variable = "\@_DUMMY_" . $print_count;

        print $init_spaces . "BEGIN\n";
        print $init_spaces . "    DECLARE " . $dummy_variable . "  VARCHAR(255)  -- FIXME\n";
        print $init_spaces . "    SELECT "  . $dummy_variable . " = " . $out_string . "\n";
        print $init_spaces . "    PRINT "   . $dummy_variable . "\n";
        print $init_spaces . "END\n";

        $print_count++;
    }
    else {
        print;
    }

}

sub safe_quote {
    my($in_string) = shift;

    my($inside_single);
    my($inside_double);
    my($i);
    my($out_string);
    my($char);

    $inside_single = 0;
    $inside_double = 0;
    $i             = 0;
    $out_string    = '';

  CHAR:
    for($i = 0; $i < length($in_string); $i++) {
        $char = substr($in_string, $i, 1);

        if ($char =~ /\'/) {

            if ($inside_single) { # End of a regular single quoted string
                $inside_single  = 0;
                $out_string    .= $char;
                next CHAR;
            }

            if ($inside_double) { # In the middle of what *was* a double quoted string, toggle this to double.
                $out_string .= '"';
                next CHAR;
            }

            $inside_single  = 1;
            $out_string    .= $char;

            next CHAR;
        }

        if ($char =~ /\"/) {

            if ($inside_single) { # In the middle of a regular quoted string...
                $out_string .= $char;
                next CHAR;
            }

            if ($inside_double) { # This is the end of what *was* double quoted string,
                $inside_double  = 0;
                $out_string    .= "'";
                next CHAR;
            }

            $inside_double  = 1;
            $out_string    .= "'";

            next CHAR;
        }

        $out_string .= $char;
    }

    return $out_string;
}

