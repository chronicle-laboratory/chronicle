---------------- MODULE UnreliableNetwork ---------------
EXTENDS FiniteSets, Sequences, Integers, TLC

CONSTANT LossBudget

VARIABLES
    message_channel,
    message_fail_count


UCSendToEnsemble(messages) ==
    /\ \A message \in messages : message \notin DOMAIN message_channel
    /\ \E loss_map \in [messages -> {-1, 1}] :
        /\ \A message \in messages :
                \/ LossBudget = 0
                \/ (message \in DOMAIN message_fail_count /\ message_fail_count[message] >= LossBudget)
                => (loss_map[message] = 1)
        /\ message_channel' = message_channel @@ loss_map
        /\ LET
           UpdateOld == [
                                                              message \in DOMAIN message_fail_count |-> IF /\ message \in messages
                                                                                                           /\ loss_map[message] = -1
                                                                                                           /\ message_fail_count[message] < LossBudget
                                                                                                        THEN message_fail_count[message] + 1
                                                                                                        ELSE message_fail_count[message]
                                                           ]
           NewFailure == [message_key \in {message \in messages : /\ message \notin DOMAIN message_fail_count
                                                                    /\ loss_map[message] = -1
                                            }
                                            |-> 1
                           ]
           IN
            /\ message_fail_count' = NewFailure @@ UpdateOld


UCAckMessage(message) ==
    /\ message \in DOMAIN message_channel
    /\ message_channel[message] >= 1
    /\ message_channel' = [m \in DOMAIN message_channel \ {message} |-> message_channel[m]]


UCConsumeAndSend(consume_msg, send_msg) ==
    /\ consume_msg \in DOMAIN message_channel
    /\ send_msg \notin DOMAIN message_channel \/ send_msg = consume_msg
    /\ LET
           base_chan == [m \in DOMAIN message_channel \ {consume_msg} |-> message_channel[m]]
           force_success == \/ /\ send_msg \in DOMAIN message_fail_count
                               /\ message_fail_count[send_msg] >= LossBudget
                            \/ LossBudget = 0
       IN
        \E loss_factor \in {-1, 1} :
            LET next_status == IF force_success THEN 1 ELSE loss_factor
                next_fail_count ==
                    IF next_status = -1
                    THEN IF send_msg \in DOMAIN message_fail_count
                         THEN [message_fail_count EXCEPT ![send_msg] = @ + 1]
                         ELSE (send_msg :> 1) @@ message_fail_count
                    ELSE message_fail_count
            IN
                /\ message_channel' = base_chan @@ (send_msg :> next_status)
                /\ message_fail_count' = next_fail_count

========================================================
