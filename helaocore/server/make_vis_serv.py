__all__ = ["makeVisServ"]


from .api import HelaoBokehAPI
from .vis import Vis


def makeVisServ(
    config,
    server_key,
    doc,
    server_title,
    description,
    version,
    driver_class=None,
):
    app = HelaoBokehAPI(
        helao_cfg=config,
        helao_srv=server_key,
        doc=doc,
        title=server_title,
        description=description,
        version=version,
    )
    app.vis = Vis(app)
    return app
