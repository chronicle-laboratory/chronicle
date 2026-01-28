------------------- MODULE Chronicle -------------------
EXTENDS FiniteSets, Sequences, Integers, TLC, UnreliableNetwork, Catalog, Unit, Timeline


CONSTANTS
    \* messages
    RecordEventRequest,
    RecordEventResponse,
    FetchEventsRequest,
    FetchEventsResponse,
    FenceRequest,
    FenceRespons,
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

    Timelines,
    TimelineWQ,
    TimelineAQ,
    Events,

    \* timeline status
    TimelineStatusOpen,
    TimelineStatusInRecovery,
    TimelineStatusClosed,

    \* timeline type
    TimelineBasic,
    TimelineCompacted

ASSUME TimelineWQ \in Nat /\ TimelineWQ > 0
ASSUME TimelineAQ \in Nat /\ TimelineAQ > 0
ASSUME TimelineWQ >= TimelineAQ
ASSUME Cardinality(Events) + Cardinality(Timelines) >= 1

VARIABLES
    \* catalog
    catalog_timeline_status,
    catalog_timeline_type,
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
    acked_events


unit_variables == << unit_timeline_events, unit_timeline_lra, unit_timeline_lrfa, unit_timeline_term >>
catalog_variables == << catalog_timeline_term, catalog_timeline_lra, catalog_timeline_segments, catalog_timeline_status, catalog_timeline_type, catalog_timeline_version >>
vars == << unit_variables, catalog_variables, message_channel, timelines, sent_events, acked_events>>


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




FindLast(seq) == seq[Len(seq)]

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
        /\ UCAckAndSendAnother(message, ResRecordEvent(message))
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
                        /\ UCAckMessage(message)
        /\ UNCHANGED << unit_variables, catalog_variables, sent_events>>

Timeline ==
    [
        id                       : Timelines,
        term                     : Nat,
        segments                 : [Nat -> Segment],
        writable_segment         : Segment \union {Null},
        inflight_records         : SUBSET InflightEvent,
        status                   : TimelineStatus,
        lrs                      : Nat,
        lra                      : Nat,
        lrfa                     : Nat,
        acked                    : [EventOffsets -> SUBSET Units],
        fenced                   : SUBSET Units,
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


IsValidEnsemble(catalog_timeline_segments, ensemble, include_units, exclude_units) ==
    /\ Cardinality(ensemble) = TimelineWQ
    /\ ensemble \intersect exclude_units = {}
    /\ include_units \intersect ensemble = include_units
    \* Ensures monotonic state evolution by excluding the set of previously allocated ensembles,
    \* thereby avoiding livelocks and providing a physical distinction between log segments.
    /\ \A i \in DOMAIN catalog_timeline_segments :
        ensemble # catalog_timeline_segments[i].ensemble


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

=========================================================