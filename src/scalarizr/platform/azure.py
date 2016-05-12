from scalarizr.platform import Platform


def get_platform():
    return AzurePlatform()


class AzurePlatform(Platform):
    name = "azure"
    features = []
