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

    Timelines,
    TimelineWQ,
    TimelineAQ,
    Events,

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
    timelines


vars == << units, catalogs, message_channel, timelines >>



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
        writable_segment         : Segment \union {Null},
        inflight_record_event_req: SUBSET InflightEvent,
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

FindEnsemble(tid, available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units : IsValidEnsemble(ensemble, available, quarantined)

(***************************************************************************
ACTION: OPEN NEW TIMELINE
***************************************************************************)

OpenNewTimeline(tid) ==
    /\  LET timeline == timelines[tid]
            timeline_catalog == catalogs[tid]
        IN
             /\ timeline_catalog.version = Null
             /\ timeline.catalog_version = Null
             /\ LET writable_segment == [id |-> 1, ensemble |->  FindEnsemble(tid, {}, {}), start_offset |-> 1]
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
                     /\ UNCHANGED << units, message_channel>>



(***************************************************************************
ACTION: RECROD EVENT
***************************************************************************)

ReqRecordEvent(timeline, event, ensemble, trunc) ==
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
    /\ LET timeline == timelines[tid]
           payload == CHOOSE payload \in Events : payload
           event == [offset |-> timeline.lrs + 1, data |-> payload]
       IN
            /\ UCSendToEnsemble(ReqRecordEvent(timeline, event, timeline.writable_segment.ensemble, FALSE))
            /\ timelines' = [timelines EXCEPT ![timeline.id] = [
                                            @ EXCEPT !.lrs = timeline.lrs + 1,
                                                     !.inflight_record_event_req = @ \union {
                                                            [
                                                                event      |->  [offset |-> timeline.lrs + 1, data |-> payload],
                                                                segment_id |->  timeline.writable_segment.id,
                                                                ensemble   |->  timeline.writable_segment.ensemble
                                                            ]
                                                     }
                                                 ]
                             ]
        /\ UNCHANGED << units, catalogs >>

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
        /\ units[message.unit].term <= message.term
        /\ units' = [units EXCEPT ![message.unit] = [
                            @ EXCEPT !.term     = message.term,
                                     !.events   = IF message.trunc
                                                  THEN {event \in @ : event.offset < message.event.offset } \union message.event
                                                  ELSE (@ \ {event \in @ : event.offset = message.event.offset}) \union {message.event},
                                     !.lra      = IF message.lra  > @ THEN message.lra  ELSE @,
                                     !.lrfa     = IF message.lrfa > @ THEN message.lrfa ELSE @

                            ]
                     ]
        /\ UCAckAndSendAnother(message, ResRecordEvent(message))
        /\ UNCHANGED << timelines, catalogs>>


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
        /\ message.unit             \in timelines[tid].writable_segment.ensemble
        /\ LET
                timeline == timelines[tid]
                event    == message.event
                acked    == [timeline.acked EXCEPT ![event.offset] = @ \union {message.unit}]
                lra      == GetAckedOffset(timeline, acked, TimelineAQ)
                lrfa     == GetAckedOffset(timeline, acked, TimelineWQ)
           IN
            /\ timelines' = [timelines EXCEPT ![timeline.id] = [
                                        timeline EXCEPT !.acked                     = acked,
                                                        !.lra                       = IF lra  > @ THEN lra ELSE @,
                                                        !.lrfa                      = IF lrfa > @ THEN lrfa ELSE @,
                                                        !.inflight_record_event_req = {op \in timeline.inflight_record_event_req : op.event.offset > lrfa}
                                        ]
                            ]
            /\ UCAckMessage(message)
    /\ UNCHANGED << units, catalogs>>


InitTimeline(tid) ==
    [
        id                                  |-> tid,
        term                                |-> 0,
        segments                            |-> <<>>,
        writable_segment                    |-> Null,
        inflight_record_event_req           |-> {},
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

Next ==
    \/ UnitHandleReqRecordEvent
    \/ \E tid \in Timelines :
        \/ OpenNewTimeline(tid)
        \/ TimelineRecordEvent(tid)
        \/ TimelineHandleRecordEvenResponse(tid)

TypeOK ==
    /\ units \in [Units -> Unit]
    /\ catalogs \in [Timelines -> TimelineCatalog]
    /\ timelines \in [Timelines -> Timeline]

Spec == Init /\ [][Next]_vars

=========================================================