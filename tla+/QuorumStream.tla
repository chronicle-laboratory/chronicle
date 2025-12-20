------- MODULE QuorumStream ---------

EXTENDS Integers, Sequences, FiniteSets



CONSTANTS
    Units,   \* {u1, u2, u3, u4}
    Events   \* {e1, e2, e3}

VARIABLES
    unit_storage,
    network,
    metadata


Vars == <<unit_storage, network>>


TypeOk  ==
    /\ unit_storage \in [Units -> SUBSET Events]
    /\ network \in SUBSET [
        type: {""}
    ]



=====================================