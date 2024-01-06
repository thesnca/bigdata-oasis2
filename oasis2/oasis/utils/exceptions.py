from oasis.utils.generator import gen_uuid4


class OasisException(Exception):
    """Base Exception for the project

    To correctly use this class, inherit from it and define
    a 'message' and 'code' properties.
    """
    message = "An unknown exception occurred"
    code = "UNKNOWN_EXCEPTION"

    def __str__(self):
        return self.message

    def __init__(self, message=None, code=None, inject_error_id=True):
        self.uuid = gen_uuid4()

        if code:
            self.code = code
        if message:
            self.message = message

        if inject_error_id:
            # Add Error UUID to the message if required
            self.message = f'{self.message}\nError ID: {self.uuid}'

        super(OasisException, self).__init__(f'{self.code}: {self.message}')


class ChargeException(OasisException):
    message = "charge exception"

    def __init__(self, message=None):
        self.code = "CHARGE_FAIL"
        if message:
            self.message = message
        super(ChargeException, self).__init__()


class VpcRequestException(OasisException):
    message = "vpc request exception"

    def __init__(self, message=None):
        self.code = "VPC_REQUEST_FAIL"
        if message:
            self.message = message
        super(VpcRequestException, self).__init__()


class KecRequestException(OasisException):
    message = "kec request exception"

    def __init__(self, message=None):
        self.code = "KEC_REQUEST_FAIL"
        if message:
            self.message = message
        super(KecRequestException, self).__init__()


class EpcRequestException(OasisException):
    message = "epc request exception"

    def __init__(self, message=None):
        self.code = "EPC_REQUEST_FAIL"
        if message:
            self.message = message
        super(EpcRequestException, self).__init__()


class EbsRequestException(OasisException):
    message = "ebs request exception"

    def __init__(self, message=None):
        self.code = "EBS_REQUEST_FAIL"
        if message:
            self.message = message
        super(EbsRequestException, self).__init__()


class VpcNeutronException(OasisException):
    message = "vpc neutron exception"

    def __init__(self, message=None):
        self.code = "vpc neutron error"
        if message:
            self.message = message
        super(VpcNeutronException, self).__init__()


class UserPermissionError(Exception):
    pass


class ValidationError(Exception):
    pass


class ResourceCheckError(Exception):
    pass
