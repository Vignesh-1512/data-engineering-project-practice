class PipelineException(Exception):
    """Base class for pipeline errors"""
    pass


class LayerNotFoundException(PipelineException):
    pass


class DatasetNotFoundException(PipelineException):
    pass


class SchemaBuildException(PipelineException):
    pass
