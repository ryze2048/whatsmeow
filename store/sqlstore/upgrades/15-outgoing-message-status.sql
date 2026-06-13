-- v15 (compatible with v8+): Store outgoing message delivery/read status
CREATE TABLE whatsmeow_outgoing_message_status (
	our_jid             TEXT   NOT NULL,
	chat_jid            TEXT   NOT NULL,
	recipient_jid       TEXT   NOT NULL,
	message_id          TEXT   NOT NULL,
	status              TEXT   NOT NULL CHECK ( status IN ('pending', 'delivered', 'read') ),
	sent_timestamp      BIGINT NOT NULL,
	delivered_timestamp BIGINT,
	read_timestamp      BIGINT,

	PRIMARY KEY (our_jid, chat_jid, recipient_jid, message_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
