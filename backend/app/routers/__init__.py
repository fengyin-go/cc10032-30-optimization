"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import checkpoint_retention, resource

# 更具体的路径必须先注册：/api/checkpoint-retention/... 不能落到 /api/{module}/...
ROUTERS = [checkpoint_retention, resource]
