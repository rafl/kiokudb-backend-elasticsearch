use strict;
use warnings;
use Test::More;

use ElasticSearch::TestServer;
use KiokuDB;
use KiokuDB::Test;
use KiokuDB::Backend::ElasticSearch;

my $es = ElasticSearch::TestServer->new(
    instances => 1,
);

my $b = KiokuDB::Backend::ElasticSearch->new({
    es => $es,
});

run_all_fixtures( KiokuDB->new( backend => $b ) );

done_testing;
