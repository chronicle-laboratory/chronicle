--------- MODULE Timeline -----------
EXTENDS FiniteSets, Sequences, Integers, TLC


CONSTANTS
    \* messages
    RecordEventRequest,
    RecordEventResponse,
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
    TimelineStatusClosed,

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
    catalog_timeline_lra,

    \* unit
    unit_timeline_events,
    unit_timeline_lra,
    unit_timeline_lrfa,
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

unit_variables == << unit_timeline_events, unit_timeline_lra, unit_timeline_lrfa, unit_timeline_term >>
catalog_variables == << catalog_timeline_term, catalog_timeline_lra, catalog_timeline_segments, catalog_timeline_status, catalog_timeline_type, catalog_timeline_version >>
vars == << unit_variables, catalog_variables, message_channel, timelines, sent_events, acked_events>>



ReqFence(timeline, ensemble, term) ==
    {
        [
            type         |-> FenceRequest,
            unit         |-> unit,
            timeline_id  |-> timeline.id,
            term         |-> term
        ] : unit \in ensemble
    }



UCSendToEnsemble(messages) ==
    /\ \A message \in messages :  message \notin DOMAIN message_channel
    /\ LET loss_matrix == { loss_matrix \in SUBSET (messages \X {-1, 1}) :
                             /\ Cardinality(loss_matrix) = Cardinality(messages)
                             /\ \A message \in messages :
                                    \E loss_tuple \in loss_matrix : loss_tuple[1] = message }
        IN
            \E plan \in loss_matrix :
                LET chosen_messages == [
                                        message \in messages |-> LET tuple == CHOOSE tuple \in plan: tuple[1] = message IN tuple[2]
                                    ]
                IN
                    message_channel' = message_channel @@ chosen_messages


DropMessage(drop_message) ==
    /\ drop_message \in DOMAIN message_channel
    /\ message_channel[drop_message] >= 1
    /\ message_channel' = [message_channel EXCEPT ![drop_message] = @ -1]

DropAndSendAnother(drop_message, another_message) ==
    /\ drop_message \in DOMAIN message_channel
    /\ another_message \notin DOMAIN message_channel
    /\ message_channel[drop_message] >= 1
    /\ \E loss_factor \in {-1, 1} :
        /\ message_channel' = [message_channel EXCEPT ![drop_message] = @-1] @@ (another_message :> loss_factor)



(*****
    utilities function
*****)
FindLast(seq) == seq[Len(seq)]


NoReconciliation == 0
ReconciliationFencing == 1
ReconciliationAligning == 2

EventOffsets ==
    1..Cardinality(Events) + Cardinality(Timelines) -1

Event ==
    [ offset: EventOffsets, data: Events ]

NullEvent ==
     [ offset |-> 0, data |-> Null ]

Segment ==
    [ id: Nat, ensemble: SUBSET Units, start_offset: Nat]

InflightEvent ==
    [ event: Event, segment_id: Nat, ensemble: SUBSET Units]

TimelineStatus ==
    { Null, TimelineStatusOpen, TimelineStatusInRecovery, TimelineStatusClosed }

Timeline ==
    [
        id                       : Timelines,
        term                     : Nat,
        segments                 : [Nat -> Segment],
        writable_segment         : Segment \union {Null},
        inflight_records         : SUBSET InflightEvent,
        status                   : TimelineStatus,
\*      last record sent
        lrs                      : Nat,
\*      last record acked
        lra                      : Nat,
\*      last record full acked
        lrfa                     : Nat,
        acked                    : [EventOffsets -> SUBSET Units],
        fenced                   : SUBSET Units,
\*      reconciliation memory state
        reconciliation           : NoReconciliation..ReconciliationAligning,
        reconciliation_ensemble  : SUBSET Units,
        reconciliation_lra       : Nat,
        reconciliation_lrfa      : Nat,
        catalog_timeline_version   : Nat \union {Null}
    ]

InitTimeline(tid) ==
    [
        id                      |-> tid,
        term                    |-> 0,
        segments                |-> [i \in 1..0 |-> [id |-> i, ensemble |-> {}, start_offset |-> 1]],
        writable_segment        |-> Null,
        inflight_record_event_req        |-> {},
        status                  |-> Null,
        lrs                     |-> 0,
        lra                     |-> 0,
        lrfa                    |-> 0,
        acked                   |-> [offset \in EventOffsets |-> {}],
        fenced                  |-> {},
        reconciliation          |-> NoReconciliation,
        reconciliation_ensemble |-> {},
        reconciliation_lra      |-> 0,
        reonciliation_lrfa      |-> 0,
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
    LET timeline == timelines[tid]
    IN
        /\ catalog_timeline_status = Null
        /\ timeline.catalog_timeline_version = Null
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
             /\ UNCHANGED << unit_variables , catalog_timeline_lra, message_channel, sent_events, acked_events>>


\*OpenExistingTimeline(tid) ==
\*    LET
\*        catalog_timeline_new_term    == catalog_timeline_term +1
\*        catalog_timeline_new_version == catalog_timeline_version + 1
\*    IN
\*    /\ timelines[tid].status = Null
\*    /\ catalog_timeline_status \in {TimelineStatusOpen, TimelineStatusInRecovery}
\*    /\ catalog_timeline_status' = TimelineStatusInRecovery
\*    /\ catalog_timeline_version' = catalog_timeline_new_version
\*    /\ catalog_timeline_term' = catalog_timeline_new_term
\*    /\ timelines' = [
\*                        timelines EXCEPT ![tid] = [@ EXCEPT
\*                                !.status                   = TimelineStatusInRecovery,
\*                                !.catalog_timeline_version = catalog_timeline_new_version,
\*                                !.term                     = catalog_timeline_new_term,
\*                                !.reconciliation           = ReconciliationFencing,
\*                                !.segments                 = catalog_timeline_segments,
\*                                !.writable_segment         = FindLast(catalog_timeline_segments),
\*                                !.conciliation_ensemble    = FindLast(catalog_timeline_segments)
\*                        ]
\*                    ]
\*    /\ UCSendToEnsemble(ReqFence(timelines[tid], FindLast(catalog_timeline_segments).ensemble, catalog_timeline_new_term))
\*    /\ UNCHANGED << timeline_variables, catalog_timeline_segments, sent_events, acked_events>>


\*HandleResFence(tid) ==
\*    LET timeline == timelines[tid]
\*    IN /\ timeline.reconciliation = ReconciliationFencing
\*       /\ \E message \in DOMAIN message_channel :
\*             /\ message.tid  = tid
\*             /\ message.type = FenceResponse
\*             /\ message_channel[message] >= 1
\*             /\ message.term = timeline.term
\*             /\ message.code = Ok
\*             /\ LET lra == IF message.lra > timeline.writable_segment.start_offset -1
\*                           THEN message.lra
\*                           ELSE timeline.writable_segment.start_offset -1
\*                    lrfa == IF message.lrfa > timeline.writable_segment.start_offset -1
\*                           THEN message.lrfa
\*                           ELSE timeline.writable_segment.start_offset -1
\*               IN
\*                /\ timeline' = [
\*                                    timelines EXCEPT ![tid] =
\*                                        [
\*                                            @ EXCEPT !.fenced = @ \union {message.unit},
\*                                                     !.reconciliation_lrfa = IF lrfa > @ THEN lrfa ELSE @,
\*                                                     !.reconciliation_lra = IF lra > @ THEN lra ELSE @,
\*                                                     !.lrs = IF lrfa > @ THEN lrfa ELSE @
\*                                        ]
\*                               ]
\*                /\ DropMessage(message)
\*                /\ UNCHANGED << catalog_variables, sent_events, acked_events >>
\*

(**
    Units
**)


\*\* Fencen handling
\*ResFence(req) ==
\*    [
\*        type            |-> FenceResponse,
\*        unit            |-> req.unit,
\*        timeline_id     |-> req.timeline_id,
\*        lra             |-> unit_timeline_lra[req.unit],
\*        lrfa            |-> unit_timeline_lrfa[req.unit],
\*        term            |-> req.term,
\*        code            |-> Ok
\*    ]
\*
\*
\*UnitHandleReqFence ==
\*    \E message \in DOMAIN message_channel :
\*        /\ message.type = ReqFence
\*        /\ message_channel[message] >= 1
\*        /\ unit_timeline_term[message.unit] <= message.term
\*        /\ unit_timeline_term' = [unit_timeline_term EXCEPT ![message.unit] = message.term]
\*        /\ DropAndSendAnother(message, ResFence(message))
\*        /\ UNCHANGED <<unit_timeline_event_records, unit_timeline_lra, unit_timeline_lrfa, timelines, catalog_variables, sent_events, acked_events>>



ResRecordEvent(req) ==
    [
        type            |-> RecordEventResponse,
        unit            |-> req.unit,
        timeline_id     |-> req.timeline_id,
        event           |-> req.event,
        term            |-> req.term,
        code            |-> Ok
    ]



UnitHandleReqRecordEvent ==
    \E message \in DOMAIN message_channel :
        /\ message.type = RecordEventRequest
        /\ message_channel[message] >= 1
        /\ ~\E queue_message \in DOMAIN message_channel:
                /\ queue_message.type           = RecordEventRequest
                /\ message_channel[queue_message]     >= 1
                /\ queue_message.term           = message.term
                /\ queue_message.timeline_id    = message.timeline_id
                /\ queue_message.unit           = message.unit
                /\ queue_message.event.offset   < message.event.offset
        /\ unit_timeline_term[message.unit] <= message.term
        /\ unit_timeline_term'   = [unit_timeline_term EXCEPT ![message.unit] = message.term]
        /\ unit_timeline_events' = [unit_timeline_events EXCEPT ![message.unit] =
                                                        IF message.trunc
                                                        THEN {event \in unit_timeline_events[message.unit] : event.offset < message.event.offset } \union message.event
                                                        ELSE (unit_timeline_events[message.unit] \ {event \in unit_timeline_events[message.unit] : event.offset = message.event.offset}) \union {message.event}
                                   ]
        /\ unit_timeline_lra'  = [unit_timeline_lra   EXCEPT ![message.unit] = IF message.lra  > @ THEN message.lra  ELSE @]
        /\ unit_timeline_lrfa' = [unit_timeline_lrfa EXCEPT ![message.unit] = IF message.lrfa > @ THEN message.lrfa ELSE @]
        /\ DropAndSendAnother(message, ResRecordEvent(message))
        /\ UNCHANGED << timelines, catalog_variables, sent_events, acked_events>>



OpenTimeline(tid) ==
    \/ OpenNewTimeline(tid)
\*    \/ OpenExistingTimeline(tid)



ReqRecordEvent(timeline, event, ensemble, recovery, trunc) ==
    {
        [
            type             |-> RecordEventRequest,
            unit             |-> unit,
            timeline_id      |-> timeline.id,
            event            |-> event,
            lra              |-> timeline.lra,
            lrfa             |-> timeline.lrfa,
            term             |-> timeline.term,
            trunc            |-> trunc
        ] : unit \in ensemble
    }



TimelineRecordEvent(tid) ==
    LET timeline == timelines[tid]
        IN
            /\ timeline.status = TimelineStatusOpen
            /\ \E payload \in Events : payload \notin sent_events
            /\ LET payload == CHOOSE payload \in Events : payload \notin sent_events
                IN
                    LET event == [offset |-> timeline.lrs + 1, data |-> payload]
                    IN
                        /\ UCSendToEnsemble(ReqRecordEvent(timeline, event, timeline.writable_segment.ensemble, FALSE, FALSE))
                            /\ LET t == timeline IN
                                 timelines' = [ timelines EXCEPT ![timeline.id] =
                                                             [
                                                              timeline EXCEPT
                                                                        !.lrs = timeline.lrs + 1,
                                                                        !.inflight_record_event_req = @ \union
                                                                            {
                                                                                [
                                                                                    event      |->  [offset |-> timeline.lrs + 1, data |-> payload],
                                                                                    segment_id |-> timeline.writable_segment.id,
                                                                                    ensemble   |-> timeline.writable_segment.ensemble
                                                                                ]
                                                                            }
                                                             ]
                                            ]
                        /\ sent_events' = sent_events \union {payload}
            /\ UNCHANGED << unit_variables, catalog_variables, acked_events>>




GetAckedOffset(timeline, acked, quorum) ==
   IF timeline.lra < timeline.lrs
   THEN
        IF \E offset0 \in (timeline.lra + 1)..timeline.lrs : \A offset1 \in (timeline.lra + 1)..offset0 : Cardinality(acked[offset1]) >= quorum
        THEN
            CHOOSE offset0 \in (timeline.lra + 1)..timeline.lrs :
                                /\ \A offset1 \in (timeline.lra + 1)..offset0 : Cardinality(acked[offset1]) >= quorum

                /\ ~\E offset_2 \in (timeline.lra + 1)..timeline.lrs :
                        /\ offset_2 > offset0
                        /\ \A offset_3 \in (timeline.lra + 1)..offset_2 : Cardinality(acked[offset_3]) >= quorum
        ELSE timeline.lra
   ELSE
    timeline.lra


TimelineHandleRecordEvenResponse(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusOpen
        /\ \E message \in DOMAIN message_channel :
                /\ message.type             =  RecordEventResponse
                /\ message_channel[message] >= 1
                /\ message.code             = Ok
                /\ message.unit  \in timeline.writable_segment.ensemble
                /\ LET acked    == [timeline.acked EXCEPT ![message.event.offset] = @ \union {message.unit}]
                    IN LET lra  == GetAckedOffset(timeline, acked, TimelineAQ)
                           lrfa == GetAckedOffset(timeline, acked, TimelineWQ)
                       IN
                        /\ timelines' = [timelines EXCEPT ![timeline.id] =
                                                                    [
                                                                        timeline EXCEPT !.acked               = acked,
                                                                                        !.lra                 = IF lra  > @ THEN lra ELSE @,
                                                                                        !.lrfa                = IF lrfa > @ THEN lrfa ELSE @,
                                                                                        !.inflight_record_event_req = {op \in timeline.inflight_record_event_req : op.event.offset > lrfa}
                                                                    ]
                                        ]
                        /\ acked_events' = IF lra >= message.event.offset THEN acked_events \union {message.event.data} ELSE acked_events
                        /\ DropMessage(message)
        /\ UNCHANGED << unit_variables, catalog_variables, sent_events>>


Init ==
    /\ catalog_timeline_term = 0
    /\ catalog_timeline_lra = 0
    /\ catalog_timeline_status = Null
    /\ catalog_timeline_segments = <<>>
    /\ catalog_timeline_type = TimelineBasic
    /\ catalog_timeline_version = 0
    /\ unit_timeline_term = [unit \in Units |-> 0]
    /\ unit_timeline_events = [unit \in Units |-> {}]
    /\ unit_timeline_lra = [unit \in Units |-> 0]
    /\ unit_timeline_lrfa = [unit \in Units |-> 0]
    /\ timelines = [tid \in Timelines |-> InitTimeline(tid)]
    /\ message_channel = [message \in {} |-> 0]
    /\ sent_events = {}
    /\ acked_events = {}

Next ==
    \/ UnitHandleReqRecordEvent
    \/ \E tid \in Timelines :
        \/ OpenTimeline(tid)
        \/ TimelineRecordEvent(tid)
        \/ TimelineHandleRecordEvenResponse(tid)


TypeOK ==
    /\ catalog_timeline_status      \in {Null, TimelineStatusOpen, TimelineStatusInRecovery, TimelineStatusClosed}
    /\ catalog_timeline_lra         \in Nat
    /\ catalog_timeline_version     \in Nat
    /\ unit_timeline_term           \in [Units -> Nat]
    /\ unit_timeline_events         \in [Units -> SUBSET Event]
    /\ unit_timeline_lra            \in [Units -> Nat]



Spec == Init /\ [][Next]_vars

===============================================================================