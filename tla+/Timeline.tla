--------- MODULE Timeline -----------
EXTENDS FiniteSets, Sequences, Integers, TLC


CONSTANTS
    \* messages
    RecordEventsRequest,
    RecordEventsResponse,
    FetchEventsRequest,
    FetchEventsReponse,
    FenceRequest,
    FenceResponse

CONSTANTS
    \* input
    Units,
    Clients,
    TimelineWQ,
    TimelineAQ,
    Values

CONSTANTS
   \* constants
   Ok,
   Unknown,
   InvalidTerm

VARIABLES
    \* catalog
    catalog_timeline_status,
    catalog_timeline_type,
    catalog_timeline_segments,
    catalog_timeline_term,
    catalog_timeline_version


VARIABLES
    \* unit
    timeline_event_offset_indexes,
    timeline_event_records,
    timeline_lac,
    timtline_lafc

VARIABLES
    \*
    clients

ASSUME WQ \in Nat \ {0}
ASSUME AQ \in 1..WQ




=====================================