"""hermes-event-bridge 插件包入口。

把事件外发逻辑暴露为包级 register()，供 Hermes 插件加载器导入。
"""

from .register import register  # noqa: F401

__all__ = ["register"]