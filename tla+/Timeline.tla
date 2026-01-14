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
    acked_events,
    message_channel


ASSUME TimelineWQ \in Nat /\ TimelineWQ > 0
ASSUME TimelineAQ \in Nat /\ TimelineAQ > 0
ASSUME TimelineWQ >= TimelineAQ

ASSUME Cardinality(Events) + Cardinality(Timelines) >= 1

timeline_variables == << unit_timeline_event_records, unit_timeline_lac, unit_timeline_lafc, unit_timeline_term >>
catalog_variables == << catalog_timeline_term, catalog_timeline_segments, catalog_timeline_status, catalog_timeline_type, catalog_timeline_version >>


(**
    protocol
**)

ReqFence(timeline, ensemble, term) ==
    {
        [
            type         |-> FenceRequest,
            unit         |-> unit,
            timeline_id  |-> timeline.id,
            term         |-> term
        ] : uint \in ensemble
    }


(**
    unreliable channel
**)


UCSendToEnsemble(messages) ==
    /\ \A message \in messages :  message \notin DOMAIN message_channel
    /\ LET loss_matrix == { loss_matrix \in SUBSET (messages \X {-1, 1}) :
                             /\ Cardinality(loss_matrix) = Cardinality(messages)
                             /\ \A message \in messages :
                                    \E loss_tuple \in loss_matrix : loss_tuple[1] = message }
        IN
            \E plan \in loss_matrix :
                LET choosen_messages == [
                                        message \in messages |-> LET tuple == CHOOSE tuple \in plan: tuple[1] = message IN tuple[2]
                                    ]
                IN
                    message_channel' = message_channel @@ choosen_messages


DropAndSendAnother(drop_message, another_message) ==
    /\ drop_message \in DOMAIN message_channel
    /\ another_message \notin DOMAIN message_channel
    /\ message_channel[drop_message] >= 1
    /\ \E loss_factor \in {-1, 1} :
        /\ message_channel' = [message_channel EXCEPT ![drop_message] = @-1] @@ another_message :> loss_factor



(*****
    utilities function
*****)
FindLast(seq) == seq[Len(seq)]


NoReconciliation == 0
ReconciliationFencing == 1
ReconciliationAligning == 2

EventOffsets ==
    1..Cardinality(Events)

Event ==
    [ offset: EventOffsets, data: Events ]

NullEvent ==
     [ offset |-> 0, data |-> Null ]

Segment ==
    [ id: Nat, ensemble: SUBSET Units, start_offset: Nat]

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

\*      reconciliation memory state
        reconciliation           : NoReconciliation..ReconciliationAligning,
        reconciliation_ensemble  : SUBSET Units,
        reconciliation_lac       : Nat,
        reconciliation_lafc      : Nat,
        catalog_timeline_version   : Nat \union {Null}
    ]

InitTimeline(tid) ==
    [
        id                      |-> tid,
        term                    |-> 0,
        segments                |-> [i \in 1..0 |-> [id |-> i, ensemble |-> {}, start_offset |-> 1]],
        writable_segment        |-> Null,
        inflight_records        |-> {},
        status                  |-> Null,
        las                     |-> 0,
        lac                     |-> 0,
        lafc                    |-> 0,
        acked                   |-> [offset \in EventOffsets |-> {}],
        fenced                  |-> {},
        reconciliation          |-> NoReconciliation,
        reconciliation_ensemble |-> {},
        reconciliation_lac      |-> 0,
        reonciliation_lafc      |-> 0,
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

OpenNewTimeline(tid) ==
    /\ catalog_timeline_status = Null
    /\ Timelines[tid].catalog_timeline_version = Null
    /\ LET segment == [id |-> 1, ensemble |->  FindEnsemble({}, {}), start_offset |-> 1]
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


OpenExistingTimeline(tid) ==
    LET
        catalog_timeline_new_term    == catalog_timeline_term +1
        catalog_timeline_new_version == catalog_timeline_version + 1
    IN
    /\ timelines[tid].status = Null
    /\ catalog_timeline_status \in {TimelineStatusOpen, TimelineStatusInRecovery}
    /\ catalog_timeline_status' = TimelineStatusInRecovery
    /\ catalog_timeline_version' = catalog_timeline_new_version
    /\ catalog_timeline_term' = catalog_timeline_new_term
    /\ timelines' = [
                        timelines EXCEPT ![tid] = [@ EXCEPT
                                !.status                   = TimelineStatusInRecovery,
                                !.catalog_timeline_version = catalog_timeline_new_version,
                                !.term                     = catalog_timeline_new_term,
                                !.reconciliation           = ReconciliationFencing,
                                !.segments                 = catalog_timeline_segments,
                                !.writable_segment         = FindLast(catalog_timeline_segments),
                                !.conciliation_ensemble    = FindLast(catalog_timeline_segments)
                        ]
                    ]
    /\ UCSendMessageToEnsemble(ReqFence(timelines[tid], FindLast(catalog_timeline_segments).ensemble, catalog_timeline_new_term))
    /\ UNCHANGED << timeline_variables, catalog_timeline_segments, sent_events, acked_events>>


HandleResFence(tid) ==
    LET timeline == timelines[tid]
    IN /\ timeline.reconciliation = ReconciliationFencing
       /\ \E message \in DOMAIN message_channel
             /\ message.tid  = tid
             /\ message.type = FenceResponse
             /\ message_channel[message] >= 1
             /\ message.term = timeline.term
             /\ message.code = Ok
             /\ LET lac == IF message.lac > timeline.writable_segment.start_offset -1
                           THEN message.lac
                           ELSE timeline.writable_segment.start_offset -1
                   lafc == IF message.lafc > timeline.writable_segment.start_offset -1
                           THEN message.lafc
                           ELSE timeline.writable_segment.start_offset -1
               IN
                /\ timeline' = [
                                    timelines EXCEPT ![tid] =
                                        [
                                            @ EXCEPT !.fenced = @ \union {message.unit},
                                                     !.reconciliation_lac = IF lafc > @ THEN lafc ELSE @,
                                                     !.reconciliation_lac = IF lac > @ THEN lac ELSE @,
                                                     !.lap = IF lafc > @ THEN lafc ELSE @
                                        ]
                               ]
                /\ UNCHANGED << catalog_variables, sent_events, acked_events >>


(**
    Units
**)


\* Fencen handling

ResFence(req) ==
    [
        type            |-> FenceResponse,
        unit            |-> req.unit,
        timeline_id     |-> req.timeline_id,
        lac             |-> unit_timeline_lac[req.unit],
        lafc            |-> unit_timeline_lafc[req.unit],
        term            |-> req.term,
        code            |-> Ok
    ]


UnitHandleReqFence ==
    \E message \in message_channel :
        /\ message.type = ReqFence
        /\ message_channel[message] >= 1
        /\ unit_timeline_term[message.unit] <= message.term
        /\ unit_timeline_term' = [unit_timeline_term EXCEPT ![message.unit] = message.term]
        /\ DropAndSendAnother(message, ResFence(message))
        /\ UNCHANGED <<unit_timeline_event_records, unit_timeline_lac, unit_timeline_lafc, timelines, catalog_variables, sent_events, acked_events>>




OpenTimeline(tid) ==
    \/ OpenNewTimeline(tid)
    \/ OpenExistingTimeline(tid)



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
    /\ catalog_timeline_segments = [i \in 1..0 |-> [id |-> i, ensemble |-> {}, start_offset |-> 1]]
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

Next ==
    \/ \E tid \in Timelines :
        \/ OpenTimeline(tid)


===============================================================================

=======================================