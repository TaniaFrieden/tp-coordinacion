from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
