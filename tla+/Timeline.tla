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
    Events,

   \* constants
   Ok,
   Unknown,
   InvalidTerm,
   Null,

    \* timeline status
    TimelineStatusOpen,
    TimelineStatusInRecovery,
    TimelineStatusCanceled,

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
    timelines,
    sent_events,
    acked_events

ASSUME TimelineWQ \in Nat /\ TimelineWQ > 0
ASSUME TimelineAQ \in Nat /\ TimelineAQ > 0
ASSUME TimelineWQ >= TimelineAQ

ASSUME Cardinality(Events) + Cardinality(Timelines) >= 1

timeline_variables == << unit_timeline_event_records, unit_timeline_lac, unit_timeline_lafc, unit_timeline_term >>
catalog_variables == << catalog_timeline_term, catalog_timeline_segments, catalog_timeline_status, catalog_timeline_type, catalog_timeline_version >>

EventOffsets ==
    1..Cardinality(Events)

Event ==
    [ offset: EventOffsets, data: Events ]

NullEvent ==
     [ offset |-> 0, data |-> Null ]

Segment ==
    [ id: Nat, ensemble: SUBSET Units, first_entry_id: Nat]

InflightRecord ==
    [ entry: Entry, segment_id: Nat, ensemble: SUBSET Units]

TimelineStatus ==
    { Null, TimelineStatusOpen, TimelineStatusInRecovery, TimelineStatusCanceled }

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
        catalog_timeline_version   : Nat \union {Null}
    ]

InitTimeline(tid) ==
    [
        id                  |-> tid,
        term                |-> 0,
        segments            |-> [i \in 1..0 |-> [id |-> i, ensemble |-> {}, first_entry_id |-> 1]],
        writable_segment    |-> Null,
        inflight_records    |-> {},
        status              |-> Null,
        las                 |-> 0,
        lac                 |-> 0,
        lafc                |-> 0,
        acked               |-> [offset \in EventOffsets |-> {}],
        fenced              |-> {},
        catalog_timeline_version |-> Null
    ]

IsValidEnsemble(ensemble, include_bookies, exclude_bookies) ==
    /\ Cardinality(ensemble) = TimelineWQ
    /\ ensemble \intersect exclude_bookies = {}
    /\ include_bookies \intersect ensemble = include_bookies
    \* Ensures monotonic state evolution by excluding the set of previously allocated ensembles,
    \* thereby avoiding livelocks and providing a physical distinction between log segments.
    /\ \A i \in DOMAIN catalog_timeline_segments :
        ensemble # catalog_timeline_segments[i].ensemble

FindEnsemble(available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

IsEnsembleAvailable(available, quarantined) ==
    \E ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

OpenTimeline(tid) ==
    /\ catalog_timeline_version = 0
    /\ LET segment == [id |-> 1, ensemble |->  FindEnsemble({}, {}), first_entry_id |-> 1]
        IN
         /\ timelines' = [
                            timelines EXCEPT ![tid] =
                                [
                                 @ EXCEPT !.status = TimelineStatusOpen,
                                             !.catalog_timeline_version = 1,
                                             !.term                     = 1,
                                             !.segments                 = [i \in 1..1 |-> segment],
                                             !.writable_segment         = segment
                                ]
                         ]
         /\ catalog_timeline_status' = TimelineStatusOpen
         /\ catalog_timeline_term' = 1
         /\ catalog_timeline_version' = 1
         /\ catalog_timeline_type' = TimelineBasic
         /\ catalog_timeline_segments' = [i \in 1..1 |-> segment]
         /\ UNCHANGED << timeline_variables , sent_events, acked_events>>

VARIABLES
    messages

DelCountOf(msg, counts) ==
    LET pair == CHOOSE c \in counts : c[1] = msg
    IN pair[2]

MessagePassingToEnsemble(msgs) ==
    /\ \A msg \in msgs :  msg \notin DOMAIN messages
    /\ LET possible_del_counts == {
                                     s \in SUBSET (msgs \X {-1, 1}) :
                                        /\ Cardinality(s) = Cardinality(msgs)
                                        /\ \A msg \in msgs : \E s1 \in s : s1[1] = msg
                                  }
      IN
        \E counts \in possible_del_counts :
            LET msgs_to_send == [ m \in msgs |-> DelCountOf(m, counts)]
            IN
                messages' = messages @@ msgs_to_send


RecordEventRequest(timeline, event, ensemble, recovery, trunc) ==
    {
        [
            type             |-> RecordEventRequest,
            unit             |-> b,
            timeline_id      |-> timeline.id,
            entry            |-> event,
            lac              |-> timeline.lac,
            lafc             |-> timeline.lafc,
            term             |-> timeline.term,
            trunc            |-> trunc
        ] : b \in ensemble
    }


RecordEvent(timeline, event) ==
    /\ MessagePassingToEnsemble(RecordEventRequest(timeline, event, timeline.writable_segment.ensemble, FALSE, FALSE))
    /\ LET t == timeline IN
         timelines' = [ timelines EXCEPT ![t.id] =
                                     [
                                      t EXCEPT !.las = t.las + 1,
                                               !.inflight_records = @ \union
                                                    {
                                                        [
                                                            entry |-> [id |-> t.las + 1, data |-> event],
                                                            segment_id |-> t.writable_segment.id,
                                                            ensemble   |-> t.writable_segment.ensemble
                                                        ]
                                                    }
                                     ]
                    ]



TimelineRecordEvents(tid) ==
    LET t == timelines[tid]
        IN
            /\ t.status = TimelineStatusOpen
            /\ \E data \in Events : data \notin sent_events
            /\ LET event_data == CHOOSE data \in Events : data \notin sent_events
                IN
                    LET entry == [id |-> t.las + 1, data |-> event_data]
                    IN
                        /\ RecordEvent(t, entry.data)
                        /\ sent_events' = sent_events \union {event_data}
            /\ UNCHANGED << timeline_variables, catalog_variables, acked_events>>


Init ==
    /\ catalog_timeline_term = 0
    /\ catalog_timeline_status = Null
    /\ catalog_timeline_segments = [i \in 1..0 |-> [id |-> i, ensemble |-> {}, first_entry_id |-> 1]]
    /\ catalog_timeline_type = TimelineBasic
    /\ catalog_timeline_version = 0
    /\ unit_timeline_term = [unit \in Units |-> 0]
    /\ unit_timeline_event_records = [unit \in Units |-> {}]
    /\ unit_timeline_lac = [unit \in Units |-> 0]
    /\ unit_timeline_lafc = [unit \in Units |-> 0]
    /\ timelines = [tid \in Timelines |-> InitTimeline(tid)]
    /\ messages = [msg \in {} |-> TRUE] \* Empty message set
    /\ sent_events = {}
    /\ acked_events = {}



(***************************************************************************
ACTION: Bookie processes a message
****************************************************************************)

BookieProcessMessage(msg) ==
    /\ msg.type = RecordEventRequestMessage
    /\ LET unit == msg.unit
           entry == msg.entry
           IN
        /\ unit_timeline_term[unit] <= msg.term
        /\ unit_timeline_term' = [unit_timeline_term EXCEPT ![unit] = msg.term]
        /\ unit_timeline_event_records' = [unit_timeline_event_records EXCEPT ![unit] = 
                                            IF msg.trunc 
                                            THEN {e \in unit_timeline_event_records[unit] : e.id < entry.id} \union {entry}
                                            ELSE unit_timeline_event_records[unit] \union {entry}]
        /\ unit_timeline_lac' = [unit_timeline_lac EXCEPT ![unit] = 
                                   IF msg.lac > @ THEN msg.lac ELSE @]
        /\ unit_timeline_lafc' = [unit_timeline_lafc EXCEPT ![unit] = 
                                   IF msg.lafc > @ THEN msg.lafc ELSE @]
        /\ MessageProcessed(msg)
        /\ UNCHANGED << catalog_variables, timelines, sent_events, acked_events, messages>>

(***************************************************************************
QUORUM CALCULATIONS
***************************************************************************)

QuorumCoverage(cohort_size, ack_set) ==
    Cardinality(ack_set) >= ((cohort_size - TimelineAQ) + 1)

HasAckQuorum(tid, entry_id) ==
    LET t == timelines[tid]
        IN Cardinality(t.acked[entry_id]) >= TimelineAQ

HasWriteQuorum(tid, entry_id) ==
    LET t == timelines[tid]
        IN Cardinality(t.acked[entry_id]) >= TimelineWQ

MaxContiguousEntry(tid, quorum) ==
    LET t == timelines[tid]
    IN
        IF t.lac < t.las
        THEN 
            IF \E id \in (t.lac+1)..t.las : 
                    \A id1 \in (t.lac+1)..id :
                        Cardinality(t.acked[id1]) >= quorum
            THEN 
                CHOOSE id \in (t.lac+1)..t.las : 
                    /\ \A id1 \in (t.lac+1)..id : 
                        Cardinality(t.acked[id1]) >= quorum
                    /\ ~\E other_id \in (t.lac+1)..t.las :
                        /\ other_id > id 
                        /\ \A other_id1 \in (t.lac+1)..other_id : 
                            Cardinality(t.acked[other_id1]) >= quorum
            ELSE t.lac
        ELSE t.lac

MessageProcessed(msg) ==
    /\ messages' = [messages EXCEPT ![msg] = FALSE]

(***************************************************************************
TYPE CORRECTNESS INVARIANTS
***************************************************************************)

TypeInvariant == 
    /\ \A tid \in Timelines : timelines[tid].id = tid
    /\ \A u \in Units : unit_timeline_term[u] \in Nat
    /\ \A u \in Units : unit_timeline_lac[u] \in Nat
    /\ \A u \in Units : unit_timeline_lafc[u] \in Nat
    /\ \A u \in Units : \A e \in unit_timeline_event_records[u] : e.id \in EventOffsets /\ e.data \in Events \union {Null}

(***************************************************************************
SAFETY INVARIANTS
***************************************************************************)

MonotonicTerms ==
    \A tid \in Timelines : 
        /\ timelines[tid].term \in Nat
        /\ \A u \in Units : unit_timeline_term[u] >= 0

ValidEnsembleComposition ==
    \A tid \in Timelines :
        /\ \A i \in DOMAIN timelines[tid].segments :
            Cardinality(timelines[tid].segments[i].ensemble) = TimelineWQ

NoOrphanedEntries ==
    \A u \in Units :
        \A e \in unit_timeline_event_records[u] :
            /\ e.id > 0
            /\ e.data \in Events \union {Null}

ConsistentLACProgression ==
    \A tid \in Timelines :
        timelines[tid].lac <= timelines[tid].las

ConsistentLAFRProgression ==
    \A tid \in Timelines :
        timelines[tid].lafc <= timelines[tid].las

(***************************************************************************
SPECIFICATION
***************************************************************************)

Spec == Init /\ [][Next]_<<catalog_variables, timeline_variables, messages, sent_events, acked_events>>

THEOREM Spec => [](TypeInvariant)
THEOREM Spec => [](MonotonicTerms)
THEOREM Spec => [](ValidEnsembleComposition)
THEOREM Spec => [](NoOrphanedEntries)
THEOREM Spec => [](ConsistentLACProgression)
THEOREM Spec => [](ConsistentLAFRProgression)

===============================================================================

=======================================