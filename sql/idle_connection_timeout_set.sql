-- https://stackoverflow.com/questions/12391174/how-to-close-idle-connections-in-postgresql-automatically/12403753#12403753
--alter system set idle_in_transaction_session_timeout='5min';
--alter system set idle_in_transaction_session_timeout=0;
show idle_in_transaction_session_timeout;