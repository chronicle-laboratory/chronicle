--------- MODULE Timeline -----------
EXTENDS FiniteSets, Sequences, Integers, TLC

CONSTANTS
    \* messages
    RecordEventsRequest,
    RecordEventsResponse,
    FetchEventsRequest,
    FetchEventsResponse,
    FenceRequest,
    FenceResponse,

    \* input
    Units,
    Timelines,
    TimelineWQ,
    TimelineAQ,
    Values,

   \* constants
   Ok,
   Unknown,
   InvalidTerm,
   Null,

    \* timeline status
    TimelineOpen,
    TimelineInRecovery,
    TimelineCanceled,

    \* timeline type
    TimelineBasic,
    TimelineCompacted

VARIABLES
    \* catalog
    catalog_timeline_status,
    catalog_timeline_type,          \* basic,compacted
    catalog_timeline_segments,
    catalog_timeline_term,
    catalog_timeline_version,

    \* unit
    unit_timeline_event_records,
    unit_timeline_lac,
    unit_timeline_lafc,
    unit_timeline_term,

    \* timelines
    timelines

ASSUME WQ \in Nat /\ WQ > 0
ASSUME AQ \in Nat /\ AQ > 0
ASSUME WQ >= AQ

ASSUME Cardinality(Values) + Cardinality(Timelines) >= 1

timeline_variables == << timeline_event_offset_indexes, timeline_event_records, timeline_lac, timeline_lafc >>
catalog_variables == << catalog_timeline_term, catalog_timeline_segments, catalog_timeline_status, catalog_timeline_type, catalog_timeline_version >>

EventOffsets ==
    1..Cardinality(Values) + Cardinality(Timelines) - 1

Event ==
    [ offset: EventOffsets, data: Values ]

NullEvent ==
     [ offset |-> 0, data |-> Null ]

Segment ==
    [ offset: Nat, ensemble: SUBSET Units, start_offset: Nat]

InflightRecord ==
    [ event: Event, segment_id: Nat, ensemble: SUBSET Units]

TimelineStatus ==
    { Null, TimelineOpen, TimelineInRecovery, TimelineCanceled }

Timeline ==
    [
        id                       : Timelines,
        term                     : Nat,
        segments                 : [Nat -> Segment],
        writable_segment         : Segment \union {Null},
        inflight_records         : SUBSET InflightRecord,
        status                   : TimelineStatus,
        las                      : Nat,
        lac                      : Nat,
        lafc                     : Nat,
        acked                    : [EventOffsets -> SUBSET Units],
        fenced                   : SUBSET Units,

        \* catalog status
        catalog_timeline_version : Nat \union {Null}
    ]

InitTimeline(tid) ==
    [
        id                  |-> tid,
        term                |-> 0,
        segments            |-> <<>>,
        writable_segment    |-> Null,
        inflight_records    |-> {},
        status              |-> Null,
        las                 |-> 0,
        lac                 |-> 0,
        lafc                |-> 0,
        acked               |-> [offset \in EventOffsets |-> {}],
        fenced              |-> {}
    ]

IsValidEnsemble(ensemble, chosen, quarantined) ==
    /\ Cardinality(ensemble) = TimelineWQ
    /\ ensemble \intersect quarantined = {}
    /\ ensemble \intersect chosen = chosen
    \* Ensures monotonic state evolution by excluding the set of previously allocated ensembles,
    \* thereby avoiding livelocks and providing a physical distinction between log segments.
    /\ \A i \in DOMAIN catalog_timeline_segments :
        ensemble # catalog_timeline_segments[i].ensemble

FindEnsemble(available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

IsEnsembleAvailable(ensemble, avaialble, quarantined) ==
    \E ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, avaialble, quarantined)

OpenTimeline(tid) ==
    /\ catalog_timeline_version = Null
    /\ Timelines[tid].catalog_timeline_version = Null
    /\ LET segment == [offset |-> 1, ensemble |->  FindEnsemble({}, {}), start_offset |-> 1]
        IN
         /\ timelines' = [
                            timelines EXCEPT ![tid] =
                                [
                                 @ EXCEPT !.status = TimelineOpen,
                                             !.catalog_timeline_version = 1,
                                             !.term                     = 1,
                                             !.segments                 = Append(catalog_timeline_segments, segment),
                                             !.writable_segment         = segment
                                ]
                         ]
         /\ catalog_timeline_status' = TimelineOpen
         /\ catalog_timeline_term' = 1
         /\ catalog_timeline_version' = 1
         /\ catalog_timeline_type' = TimelineBasic
         /\ catalog_timeline_segments' = Append(catalog_timeline_segments, segment)

Init ==
    /\ catalog_timeline_term = 0
    /\ catalog_timeline_status = Null
    /\ catalog_timeline_segments = <<>>
    /\ catalog_timeline_type = TimelineBasic
    /\ catalog_timeline_version = 0
    /\ unit_timeline_term = [unit \in Units |-> 0]
    /\ unit_timeline_event_records = [unit \in Units |-> {}]
    /\ unit_timeline_lac =  [unit \in Units |-> 0]
    /\ unit_timeline_lafc = [unit \in Units |-> 0]
    /\ timelines = [tid \in Timelines |-> InitTimeline(tid)]



Next ==
    \/ \E tid \in Timelines :
        \/ OpenTimeline(tid)

=======================================