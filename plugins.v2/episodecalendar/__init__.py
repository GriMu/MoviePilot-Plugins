""" 追剧日历 - 每日定时发送今日更新通知 """
import datetime
from typing import Any, List, Dict, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType


class EpisodeCalendar(_PluginBase):
    # 插件基础信息
    plugin_name = "追剧日历"
    plugin_desc = "每日定时发送今日追剧日历通知，附带海报图片，一目了然今天哪些剧更新。"
    
    # === 补全元数据（解决无作者信息问题） ===
    plugin_author = "GriMu"
    plugin_version = "1.0.0"
    plugin_url = "https://github.com/GriMu"
    # ====================================
    
    plugin_order = 26
    is_clone = False

    # 私有属性
    _enabled: bool = False
    _cron: str = "30 8 * * *"
    _onlyonce: bool = False
    _msgtype: bool = True  # True = 插件消息，False = 系统消息
    
    # === 新增：状态记录 ===
    _last_run_time: Optional[str] = None
    _last_run_count: int = 0
    _last_run_msg: str = "等待运行"
    # ====================

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    # 配置键名
    _config_keys = ["_enabled", "_cron", "_onlyonce", "_msgtype", "_last_run_time", "_last_run_count", "_last_run_msg"]

    def init_plugin(self, config: dict = None):
        if config:
            for key in self._config_keys:
                if key in config:
                    setattr(self, key, config.get(key, getattr(self, key)))
        
        # 停止已有调度器
        self.stop_service()
        
        if self._onlyonce:
            self._onlyonce = False
            self.__update_config()
            self._run_calendar()
            return

        if self._enabled:
            self._start_scheduler()

    def _start_scheduler(self):
        """启动定时调度器"""
        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            try:
                self._scheduler.add_job(
                    func=self._run_calendar,
                    trigger=CronTrigger.from_crontab(self._cron, timezone=settings.TZ),
                    id="episode_calendar",
                    name="追剧日历",
                    replace_existing=True,
                )
                self._scheduler.start()
                logger.info("追剧日历定时任务已启动")
            except Exception as e:
                logger.error(f"追剧日历定时任务启动失败: {e}")

    def _run_calendar(self):
        """执行追剧日历推送"""
        self._last_run_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("追剧日历开始执行...")
        
        # 获取今天的日期，根据系统配置的时区
        try:
            tz = settings.TZ
            if isinstance(tz, str):
                import pytz
                tz = pytz.timezone(tz)
            today = datetime.datetime.now(tz).date()
        except Exception as e:
            logger.warning(f"时区处理失败，使用本地时间: {e}")
            today = datetime.date.today()

        today_iso = today.isoformat()  # "2025-05-12"
        today_display = today.strftime("%Y年%m月%d日")
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday = weekday_names[today.weekday()]

        # 查询所有启用的订阅（state='R' 表示订阅中）
        subscribes = SubscribeOper().list(state="R")
        if not subscribes:
            logger.info("追剧日历：无启用的订阅")
            self._last_run_msg = "无启用的订阅"
            self._last_run_count = 0
            self.__update_config()
            return

        # 按 TMDB ID 和季号分组（同一部剧多季只请求一次 TMDB）
        sub_map: Dict[tuple, list] = {}
        for sub in subscribes:
            if not sub.tmdbid:
                continue
            key = (sub.tmdbid, sub.season or 1, sub.episode_group)
            sub_map.setdefault(key, []).append(sub)

        # 逐个获取剧集排期
        today_episodes: List[Dict[str, Any]] = []
        tmdb_chain = TmdbChain()
        
        for (tmdbid, season, ep_group), subs in sub_map.items():
            try:
                episodes = tmdb_chain.tmdb_episodes(
                    tmdbid=tmdbid,
                    season=season,
                    episode_group=ep_group,
                )
            except Exception as e:
                logger.warning(f"获取 {subs[0].name} 的剧集排期失败: {e}")
                continue
            
            if not episodes:
                continue

            # 筛选今天播出的集
            today_eps = []
            total_eps_with_date = 0
            for ep in episodes:
                if not ep.air_date:
                    continue
                total_eps_with_date += 1
                if ep.air_date == today_iso:
                    today_eps.append(ep)

            if not today_eps:
                continue

            sub = subs[0] # 代表订阅
            # 计算缺失集数
            lack = sub.lack_episode if sub.lack_episode is not None else total_eps_with_date

            today_episodes.append({
                "name": sub.name,
                "year": sub.year,
                "season": season or 1,
                "poster": sub.poster or sub.backdrop,
                "backdrop": sub.backdrop,
                "total_episode": total_eps_with_date,
                "lack_episode": lack,
                "today_eps": today_eps,
                "tmdbid": sub.tmdbid,
            })

        if not today_episodes:
            logger.info(f"追剧日历：{today_display} 无剧集更新")
            self._last_run_msg = f"{today_display} 无剧集更新"
            self._last_run_count = 0
            self.__update_config()
            return

        # 构建通知内容
        title = f"📺 追剧日历 | {today_display} {weekday}"
        lines = []
        for item in sorted(today_episodes, key=lambda x: x["name"]):
            ep_strs = []
            for ep in item["today_eps"]:
                ep_strs.append(f"E{ep.episode_number:02d}")
            ep_text = "、".join(ep_strs)
            s_str = f"S{item['season']:02d}"
            
            # 进度信息
            total = item["total_episode"]
            lack = item["lack_episode"]
            progress = f"已追 {total - lack}/{total} 集" if total > 0 else ""
            
            lines.append(
                f"【{item['name']}({item['year']})】{s_str}{ep_text} 更新\n"
                f" {progress}"
            )
        
        text = "\n".join(lines)
        
        # 使用第一张海报作为消息封面图
        image = None
        for item in today_episodes:
            if item.get("poster"):
                image = item["poster"]
                break
        
        self._last_run_count = len(today_episodes)
        self._last_run_msg = f"成功推送 {len(today_episodes)} 部剧集更新"
        logger.info(f"追剧日历：今日更新 {len(today_episodes)} 部剧集")
        
        self.post_message(
            mtype=NotificationType.Plugin,
            title=title,
            text=text,
            image=image,
        )
        
        # 更新状态
        self.__update_config()

    def __update_config(self):
        """更新配置"""
        self.update_config({
            key: getattr(self, key)
            for key in self._config_keys
        })

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """ 返回前端表单配置 """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': '_enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': '_onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': '_msgtype',
                                            'label': '发送插件消息（否则为系统消息）',
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': '_cron',
                                            'label': '定时执行 Cron 表达式',
                                            'placeholder': '30 8 * * *',
                                            'hint': '默认每天 8:30 执行',
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '每天定时查询所有订阅的剧集排期，如果有今天更新的剧集，会推送通知提醒你追剧。通知附带海报图片。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "_enabled": self._enabled,
            "_cron": self._cron,
            "_onlyonce": self._onlyonce,
            "_msgtype": self._msgtype,
        }

    def get_page(self) -> Optional[list]:
        """ 返回插件详情页面 """
        return [
            {
                'component': 'VContainer',
                'props': {
                    'fluid': True,
                    'class': 'pa-4'
                },
                'content': [
                    {
                        'component': 'VCard',
                        'props': {},
                        'content': [
                            {
                                'component': 'VCardTitle',
                                'props': {},
                                'content': [
                                    {
                                        'component': 'div',
                                        'props': {
                                            'class': 'text-h5'
                                        },
                                        'text': '追剧日历'
                                    }
                                ]
                            },
                            {
                                'component': 'VCardText',
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': self.plugin_desc
                                        }
                                    },
                                    {
                                        'component': 'div',
                                        'props': {
                                            'class': 'mt-4 text-subtitle-1 font-weight-bold'
                                        },
                                        'text': '运行状态'
                                    },
                                    {
                                        'component': 'VList',
                                        'props': {
                                            'lines': 'one',
                                            'density': 'compact'
                                        },
                                        'content': [
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'title': '上次运行时间',
                                                    'subtitle': self._last_run_time or '从未运行'
                                                }
                                            },
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'title': '上次运行结果',
                                                    'subtitle': self._last_run_msg or '等待运行'
                                                }
                                            },
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'title': '上次更新剧集数',
                                                    'subtitle': str(self._last_run_count)
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def stop_service(self):
        """ 停止插件服务 """
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None
            logger.info("追剧日历定时任务已停止")
