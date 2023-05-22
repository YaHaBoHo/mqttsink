class MqttSinkError(Exception):
    pass


class MqttError(MqttSinkError):
    pass


class ConfigurationError(MqttSinkError):
    pass
