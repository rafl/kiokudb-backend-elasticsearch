package KiokuDB::Backend::ElasticSearch;

use Moose;
use syntax 'method';
use KiokuDB::Entry;
use Data::Stream::Bulk::Callback;
use namespace::autoclean;

has es => (
    is       => 'ro',
    isa      => 'ElasticSearch',
    required => 1,
);

method all_entries {
    my $s = $self->es->scrolled_search(
        index       => 'entries',
        size        => 1000,
        search_type => 'scan',
    );

    return Data::Stream::Bulk::Callback->new({
        callback => sub {
            my @entries = map {
                KiokuDB::Entry->new({
                    id   => $_->{_id},
                    data => $_->{_data},
                });
            } $s->next(1000);

            return \@entries if @entries;
            return;
        },
    });
}

method clear {
    $self->es->delete_index(index => 'entries');
}

method delete (@ids_or_entries) {
    $self->es->bulk_delete([map {
        # FIXME: KiokuDB::Entry
        { id => $_, index => 'entries', type => 'entry' }
    } @ids_or_entries]);
}

method exists (@ids) {
    map { $_->{exists} }
        @{ $self->es->mget(index => 'entries', ids => \@ids) };
}

method get (@ids) {
    map {
        KiokuDB::Entry->new({
            id => $_->{_id},
            data => $_->{_source},
        })
    } $self->es->mget(index => 'entries', ids => \@ids)
}

method insert (@entries) {
    $self->es->bulk_index([map {
        {
            index => 'entries',
            type  => 'entry',
            id    => $_->id,
            data  => $_->data,
        }
    } @entries]);
}

method search ($query) {
    my $s = $self->es->scrolled_search(
        index  => 'entries',
        size   => 1000,
        queryb => $query,
    );

    return Data::Stream::Bulk::Callback->new({
        callback => sub {
            my @entries = map {
                KiokuDB::Entry->new({
                    id   => $_->{_id},
                    data => $_->{_data},
                });
            } $s->next(1000);

            return \@entries if @entries;
            return;
        },
    });

}

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query
);

__PACKAGE__->meta->make_immutable;

1;
