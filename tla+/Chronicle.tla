------------------- MODULE Chronicle -------------------
EXTENDS FiniteSets, Sequences, Integers, TLC, UnreliableNetwork

(***************************************************************************

CHRONICLE: DISTRIBUTED FLEXIBLE STREAM CONSENSUS PROTOCOL

Chronicle is a high-performance distributed consensus protocol designed for
flexible stream orchestration and storage.

***************************************************************************)
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
    TimelineTypeBasic,
    TimelineTypeCompacted

ASSUME TimelineWQ \in Nat /\ TimelineWQ > 0
ASSUME TimelineAQ \in Nat /\ TimelineAQ > 0
ASSUME TimelineWQ >= TimelineAQ
ASSUME Cardinality(Events) + Cardinality(Timelines) >= 1




(***************************************************************************)
(* VARIABLES DEFINITIONS                                                   *)
(***************************************************************************)
VARIABLES
    catalogs,
    units,
    timelines,
    sent_events,
    acked_events


vars == << units, catalogs, message_channel, timelines, sent_events, acked_events>>



(***************************************************************************)
(* COMMON TYPE DEFINITIONS                                                 *)
(***************************************************************************)

NoReconciliation == 0
ReconciliationFencing == 1
ReconciliationAligning == 2

EventOffsets ==
    1..Cardinality(Events) + Cardinality(Timelines) -1

Event ==
    [ offset: EventOffsets, data: Events ]

NullEvent ==
     [ offset |-> 0, data |-> Null ]

Version == Nat \union {Null}

Segment ==
    [ id: Nat, ensemble: SUBSET Units, start_offset: Nat]

InflightEvent ==
    [ event: Event, segment_id: Nat, ensemble: SUBSET Units]

TimelineStatus ==
    { Null, TimelineStatusOpen, TimelineStatusInRecovery, TimelineStatusClosed }

TimelineType ==
    {TimelineTypeBasic, TimelineTypeCompacted}

TimelineCatalog ==
    [
        status          : TimelineStatus,
        type            : TimelineType,
        segments        : Seq(Segment),
        term            : Nat,
        version         : Version,
        lra             : Nat
    ]

Unit ==
    [
        timeline_events      : [Timelines ->  SUBSET Event],
        timeline_lra         : [Timelines ->  Nat],
        timeline_lrfa        : [Timelines ->  Nat],
        timeline_term        : [Timelines ->  Nat]
    ]

Timeline ==
    [
        id                       : Timelines,
        term                     : Nat,
        segments                 : Seq(Segment),
        writable_segment         : Segment \cup {Null},
        inflight_record_event_reqs: SUBSET InflightEvent,
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
        catalog_version          : Version
    ]



(***************************************************************************)
(* UTILS DEFINITIONS                                                        *)
(***************************************************************************)

FindLast(seq) == seq[Len(seq)]

IsValidEnsemble(ensemble, include_units, exclude_units) ==
    /\ Cardinality(ensemble) = TimelineWQ
    /\ ensemble \intersect exclude_units = {}
    /\ include_units \intersect ensemble = include_units

FindEnsemble(available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units : IsValidEnsemble(ensemble, available, quarantined)




(***************************************************************************
ACTION: OPEN NEW TIMELINE
***************************************************************************)

OpenNewTimeline(tid) ==
    LET timeline == timelines[tid]
            timeline_catalog == catalogs[tid]
        IN
             /\ timeline_catalog.version = Null
             /\ timeline.catalog_version = Null
             /\ LET writable_segment == [id |-> 1, ensemble |->  FindEnsemble({}, {}), start_offset |-> 1]
                IN
                     /\ timelines' = [
                                        timelines EXCEPT ![tid] =
                                            [
                                             @ EXCEPT    !.status                   = TimelineStatusOpen,
                                                         !.catalog_version          = 1,
                                                         !.term                     = 1,
                                                         !.segments                 = Append(timeline_catalog.segments, writable_segment),
                                                         !.writable_segment         = writable_segment
                                            ]
                                     ]
                     /\ catalogs' = [
                                        catalogs EXCEPT ![tid] =
                                            [
                                               @ EXCEPT  !.status   = TimelineStatusOpen,
                                                         !.version  =  1,
                                                         !.term     =  1,
                                                         !.segments =  Append(timeline_catalog.segments, writable_segment)
                                            ]
                                    ]
                     /\ UNCHANGED << units, message_channel, sent_events, acked_events>>



(***************************************************************************
ACTION: RECROD EVENT
***************************************************************************)

ReqRecordEvents(timeline, event, ensemble, trunc) ==
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

ResRecordEvent(req) ==
    [
        type            |-> RecordEventResponse,
        unit            |-> req.unit,
        timeline_id     |-> req.timeline_id,
        event           |-> req.event,
        term            |-> req.term,
        code            |-> Ok
    ]

TimelineRecordEvent(tid) ==
    /\ timelines[tid].status = TimelineStatusOpen
    /\ timelines[tid].lrs - timelines[tid].lra < 1
    /\ \E payload \in Events : payload \notin sent_events
    /\ LET timeline == timelines[tid]
           payload == CHOOSE payload \in Events : payload \notin sent_events
           event == [offset |-> timeline.lrs + 1, data |-> payload]
       IN
            /\ UCSendToEnsemble(ReqRecordEvents(timeline, event, timeline.writable_segment.ensemble, FALSE))
            /\ timelines' = [timelines EXCEPT ![timeline.id] = [
                                            @ EXCEPT !.lrs = timeline.lrs + 1,
                                                     !.inflight_record_event_reqs = @ \cup {
                                                            [
                                                                event      |->  [offset |-> timeline.lrs + 1, data |-> payload],
                                                                segment_id |->  timeline.writable_segment.id,
                                                                ensemble   |->  timeline.writable_segment.ensemble
                                                            ]
                                                     }
                                                 ]
                             ]
            /\ sent_events' = sent_events \cup {payload}
        /\ UNCHANGED << units, catalogs, acked_events>>

IsEarlistReqRecordEvent(message) ==
     ~\E queue_message \in DOMAIN message_channel:
        /\ queue_message.type                 = RecordEventRequest
        /\ message_channel[queue_message]     >= 1
        /\ queue_message.term                 = message.term
        /\ queue_message.timeline_id          = message.timeline_id
        /\ queue_message.unit                 = message.unit
        /\ queue_message.event.offset         < message.event.offset

UnitHandleReqRecordEvent ==
    \E message \in DOMAIN message_channel :
        /\ message.type = RecordEventRequest
        /\ message_channel[message] >= 1
        /\ IsEarlistReqRecordEvent(message)
        /\ units[message.unit].timeline_term[message.timeline_id] <= message.term
        /\ LET unit == units[message.unit]
                new_timeline_events == [unit.timeline_events EXCEPT ![message.timeline_id] =
                                            IF message.trunc
                                            THEN {event \in @ : event.offset < message.event.offset } \cup {message.event}
                                            ELSE (@ \ {event \in @ : event.offset = message.event.offset}) \cup {message.event}]
                new_timeline_term == [unit.timeline_term EXCEPT ![message.timeline_id] = message.term]
                new_timeline_lra  == [unit.timeline_lra EXCEPT ![message.timeline_id] = IF message.lra > @ THEN message.lra ELSE @]
                new_timeline_lrfa == [unit.timeline_lrfa EXCEPT ![message.timeline_id] = IF message.lrfa > @ THEN message.lrfa ELSE @]
           IN
                units' = [units EXCEPT ![message.unit] = [
                             timeline_term |-> new_timeline_term,
                             timeline_events |-> new_timeline_events,
                             timeline_lra |-> new_timeline_lra,
                             timeline_lrfa |-> new_timeline_lrfa
                         ]]
        /\ UCAckAndSendAnother(message, ResRecordEvent(message))
        /\ UNCHANGED << timelines, catalogs, sent_events, acked_events>>


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
    /\ timelines[tid].status = TimelineStatusOpen
    /\ \E message \in DOMAIN message_channel :
        /\ message.type             =  RecordEventResponse
        /\ message_channel[message] >= 1
        /\ message.code             = Ok
        \* Accept responses from any unit in the current writable ensemble
        \* OR from units that were in a previous ensemble for inflight events
        /\ \/ message.unit \in timelines[tid].writable_segment.ensemble
           \/ \E inflight \in timelines[tid].inflight_record_event_reqs :
                /\ inflight.event.offset = message.event.offset
                /\ message.unit \in inflight.ensemble
        /\ LET
                timeline == timelines[tid]
                event    == message.event
                acked    == [timeline.acked EXCEPT ![event.offset] = @ \cup {message.unit}]
                lra      == GetAckedOffset(timeline, acked, TimelineAQ)
                lrfa     == GetAckedOffset(timeline, acked, TimelineWQ)
           IN
            /\ timelines' = [timelines EXCEPT ![timeline.id] = [
                                        timeline EXCEPT !.acked                     = acked,
                                                        !.lra                       = IF lra  > @ THEN lra ELSE @,
                                                        !.lrfa                      = IF lrfa > @ THEN lrfa ELSE @,
                                                        !.inflight_record_event_reqs = {op \in timeline.inflight_record_event_reqs : op.event.offset > lrfa}
                                        ]
                            ]
            /\ acked_events' = IF lra >= message.event.offset THEN acked_events \cup {message.event.data} ELSE acked_events
            /\ UCAckMessage(message)
    /\ UNCHANGED << units, catalogs, sent_events>>


(***************************************************************************
ACTION: RETRY INFLIGHT RECORD EVENT
***************************************************************************)

TimelineRetryInflightRecordEvent(tid) ==
    LET timeline == timelines[tid]
    IN
       /\  timeline.status = TimelineStatusOpen
       /\  \E req \in timeline.inflight_record_event_reqs :
            /\ req.segment_id # timeline.writable_segment.id
            /\ req.ensemble   # timeline.writable_segment.ensemble
            /\ ~\E a_req \in timeline.inflight_record_event_reqs :
                /\ a_req.segment_id    = req.segment_id
                /\ a_req.ensemble      = req.ensemble
                /\ a_req.event.offset  < req.event.offset
            /\ LET target_units == timeline.writable_segment.ensemble \ req.ensemble
                   replaced_req == [
                                        event       |-> req.event,
                                        segment_id  |-> timeline.writable_segment.id,
                                        ensemble    |-> timeline.writable_segment.ensemble
                                    ]
               IN
                /\ UCSendToEnsemble(ReqRecordEvents(timeline, req, target_units, FALSE))
                /\ timelines' = [timelines EXCEPT ![timeline.id] = [ timeline EXCEPT !.inflight_record_event_reqs = (@ \ {req}) \cup {replaced_req}]]
                /\ UNCHANGED << units, catalogs, sent_events, acked_events>>


(***************************************************************************
ACTION: ENSEMBLE CHANGE
***************************************************************************)


HasFailureMessage(timeline, failure_unit) ==
    \E message \in DOMAIN message_channel :
        /\ message.type                 \in {RecordEventRequest, RecordEventResponse} 
        /\ message_channel[message]     = -1
        /\ message.timeline_id          = timeline.id
        /\ message.unit                 = failure_unit
        /\ message.term                 = timeline.term

CleanupFailureMessage(timeline, failure_unit) ==
    LET NeedClear(message) ==
        /\ message.type                 \in {RecordEventRequest, RecordEventResponse}
        /\ message_channel[message]     = -1
        /\ message.timeline_id          = timeline.id
        /\ message.unit                 \in failure_unit
        /\ message.term                 = timeline.term
    IN message_channel' = [message \in DOMAIN message_channel |-> IF NeedClear(message) THEN 0 ELSE message_channel[message]]

AppendOrModifySegment(timeline, start_offset, new_ensemble) ==
   IF start_offset = timeline.writable_segment.start_offset
       THEN [timeline.segments EXCEPT ![Len(timeline.segments)].ensemble = new_ensemble]
       ELSE Append(timeline.segments, [
               id           |-> Len(timeline.segments) + 1,
               ensemble     |-> new_ensemble,
               start_offset |-> start_offset
       ])


TimelineEnsembleChange(tid) ==
    LET timeline            == timelines[tid]
        NoStaleInflightReq  ==
           /\ ~\E req \in timeline.inflight_record_event_reqs :
                   /\ \/ req.ensemble      # timeline.writable_segment.ensemble
                      \/ req.segment_id    # timeline.writable_segment.id
    IN
        /\ NoStaleInflightReq
        /\ timeline.status       = TimelineStatusOpen
        /\ \E failure_unit \in SUBSET timeline.writable_segment.ensemble :
            /\ \A unit \in failure_unit :  HasFailureMessage(timeline, failure_unit)
            /\ LET new_ensemble                             == FindEnsemble(timeline.writable_segment.ensemble \ failure_unit, failure_unit)
                   start_offset                             == timeline.lrfa + 1
                   new_segments                             == AppendOrModifySegment(timeline, start_offset, new_ensemble)
                   next_catalog_version                     == catalogs[tid].version + 1
                   FilterAcked(acked, offset)               == IF offset >= start_offset THEN timeline.acked[offset] \ failure_unit ELSE timeline.acked[offset]
                   UnitPin(_timeline, _unit, _start_offset) == IF _start_offset > _timeline.lra THEN FALSE ELSE
                                                                        \E offset \in _start_offset.._timeline.lra : _unit \in _timeline.acked[offset]
               IN
               /\  \A unit  \in failure_unit : ~UnitPin(timeline, unit, start_offset)
               /\  catalogs' = [catalogs EXCEPT ![tid] = [catalogs[tid] EXCEPT !.segments = new_segments,
                                                                          !.version  = next_catalog_version
                                                         ]
                                ]
                /\ timelines' = [timelines EXCEPT ![tid] = [timelines[tid] EXCEPT !.catalog_version = next_catalog_version,
                                                                                  !.acked           = [offset \in DOMAIN timelines[tid].acked |-> FilterAcked(timeline.acked, offset)],
                                                                                  !.segments        = new_segments

                                    ]
                ]
                /\ CleanupFailureMessage(timeline, failure_unit)
                /\ UNCHANGED <<acked_events, sent_events, units>>



InitTimeline(tid) ==
    [
        id                                  |-> tid,
        term                                |-> 0,
        segments                            |-> <<>>,
        writable_segment                    |-> Null,
        inflight_record_event_reqs           |-> {},
        status                              |-> Null,
        lrs                                 |-> 0,
        lra                                 |-> 0,
        lrfa                                |-> 0,
        acked                               |-> [offset \in EventOffsets |-> {}],
        fenced                              |-> {},
        reconciliation                      |-> NoReconciliation,
        reconciliation_ensemble             |-> {},
        reconciliation_lra                  |-> 0,
        reconciliation_lrfa                 |-> 0,
        catalog_version                     |-> Null
    ]


InitUnit(unit) ==
    [
       timeline_events      |-> [tid \in Timelines |-> {}],
       timeline_lra         |-> [tid \in Timelines |-> 0],
       timeline_lrfa        |-> [tid \in Timelines |-> 0],
       timeline_term        |-> [tid \in Timelines |-> 0]
    ]

InitCatalogs(tid) ==
    [
         status          |-> Null,
         version         |-> Null,
         type            |-> TimelineTypeBasic,
         segments        |-> <<>>,
         term            |-> 0,
         lra             |-> 0
    ]

Init ==
    /\ units            = [unit \in Units       |-> InitUnit(unit)]
    /\ timelines        = [tid \in Timelines    |-> InitTimeline(tid)]
    /\ catalogs         = [tid \in Timelines    |-> InitCatalogs(tid)]
    /\ message_channel  = [message \in {}       |-> 0]
    /\ sent_events      = {}
    /\ acked_events     = {}

Next ==
    \/ UnitHandleReqRecordEvent
    \/ \E tid \in Timelines :
        \/ OpenNewTimeline(tid)
        \/ TimelineRecordEvent(tid)
        \/ TimelineHandleRecordEvenResponse(tid)
        \/ TimelineRetryInflightRecordEvent(tid)
        \/ TimelineEnsembleChange(tid)

TypeOK ==
    /\ units \in [Units -> Unit]
    /\ catalogs \in [Timelines -> TimelineCatalog]
    /\ timelines \in [Timelines -> Timeline]

Symmetry == Permutations(Events) \cup Permutations(Units)

Spec == Init /\ [][Next]_vars

=========================================================