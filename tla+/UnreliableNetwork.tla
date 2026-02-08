---------------- MODULE UnreliableNetwork ---------------
EXTENDS FiniteSets, Sequences, Integers,TLC


VARIABLES message_channel




UCSendToEnsemble(messages) ==
    /\ \A message \in messages : message \notin DOMAIN message_channel
    /\ LET loss_matrix == { loss_matrix \in SUBSET (messages \X {-1, 1}) :
                             /\ Cardinality(loss_matrix) = Cardinality(messages)
                             /\ \A message \in messages :
                                    \E loss_tuple \in loss_matrix : loss_tuple[1] = message }
        IN
            \E plan \in loss_matrix :
                message_channel' = message_channel @@ [ message \in messages |-> LET tuple == CHOOSE tuple \in plan: tuple[1] = message IN tuple[2] ]


UCAckMessage(message) ==
    /\ message \in DOMAIN message_channel
    /\ message_channel[message] >= 1
    /\ message_channel' = [m \in DOMAIN message_channel \ {message} |-> message_channel[m]]


UCAckAndSendAnother(ack_message, another_message) ==
    /\ ack_message \in DOMAIN message_channel
    /\ another_message \notin DOMAIN message_channel
    /\ message_channel[ack_message] >= 1
    /\ \E loss_factor \in {-1, 1} :
        /\ message_channel' = [m \in DOMAIN message_channel \ {ack_message} |-> message_channel[m]] @@ (another_message :> loss_factor)


========================================================
