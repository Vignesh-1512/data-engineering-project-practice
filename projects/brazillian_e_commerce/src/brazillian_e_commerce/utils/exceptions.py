class PipelineException(Exception):
    """Base class for pipeline errors"""
    pass


class ConfigError(PipelineException):
    pass


class DataReadError(PipelineException):
    pass


class DataWriteError(PipelineException):
    pass


class TransformationError(PipelineException):
    pass
