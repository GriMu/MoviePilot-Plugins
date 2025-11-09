from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import List, Tuple, Dict, Any

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.media import MediaChain
from app.core.config import settings
from app.core.metainfo import MetaInfoPath
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.nfo import NfoReader
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType
from app.utils.system import SystemUtils


class MpScraper(_PluginBase):
    # 插件名称
    plugin_name = "MP媒体库刮削"
    # 插件描述
    plugin_desc = "定时对媒体库进行刮削，补齐缺失元数据和图片。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "2.1.1"  # 版本号更新
    # 插件作者
    plugin_author = "GriMu"
    # 作者主页
    author_url = "https://github.com/GriMu"
    # 插件配置项ID前缀
    plugin_config_prefix = "mpscraper_"
    # 加载顺序
    plugin_order = 7
    # 可使用的用户级别
    user_level = 1

    # 私有属性
    _scheduler = None
    _scraper = None
    # 限速开关
    _enabled = False
    _onlyonce = False
    _cron = None
    _mode = ""
    _scraper_paths = ""
    _exclude_paths = ""
    # 【新增】配置项：只刮削N天内新增的资源
    _scraper_days = 0

    # 退出事件
    _event = Event()

    def init_plugin(self, config: dict = None):
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._mode = config.get("mode") or ""
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""
            # 【新增】读取新配置项
            self._scraper_days = int(config.get("scraper_days") or 0)

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self._enabled or self._onlyonce:

            if self._onlyonce:
                logger.info(f"MP媒体库刮削服务，立即运行一次")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(func=self.__libraryscraper, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="MP媒体库刮削")
                # 关闭一次性开关
                self._onlyonce = False
                # 【修改】更新配置时包含新项
                self.update_config({
                    "enabled": self._enabled,
                    "onlyonce": False,
                    "cron": self._cron,
                    "mode": self._mode,
                    "scraper_paths": self._scraper_paths,
                    "exclude_paths": self._exclude_paths,
                    "scraper_days": self._scraper_days
                })
                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [{
                "id": "MpScraper",
                "name": "MP媒体库刮削",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__libraryscraper,
                "kwargs": {}
            }]
        elif self._enabled:
            return [{
                "id": "MpScraper",
                "name": "MP媒体库刮削",
                "trigger": CronTrigger.from_crontab("0 0 */7 * *"),
                "func": self.__libraryscraper,
                "kwargs": {}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # 【修改】在表单中增加天数输入框
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'scraper_days',
                                            'label': '仅刮削N天内新增',
                                            'placeholder': '0为不限制',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '覆盖模式',
                                            'items': [
                                                {'title': '不覆盖已有元数据', 'value': ''},
                                                {'title': '覆盖所有元数据和图片', 'value': 'force_all'},
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'scraper_paths',
                                            'label': '削刮路径',
                                            'rows': 5,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_paths',
                                            'label': '排除路径',
                                            'rows': 2,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '刮削路径后拼接#电视剧/电影，强制指定该媒体路径媒体类型。'
                                                    '不加默认根据文件名自动识别媒体类型。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 0 */7 * *",
            "mode": "",
            "scraper_paths": "",
            "exclude_paths": "",
            # 【修改】增加新配置项的默认值
            "scraper_days": 0
        }

    def get_page(self) -> List[dict]:
        pass

    def __libraryscraper(self):
        """
        开始刮削媒体库
        """
        if not self._scraper_paths:
            return

        # 【新增】计算时间阈值
        # 如果 scraper_days > 0，则计算N天前的时间戳
        # 否则，time_threshold 为 None，表示不限制时间
        time_threshold = None
        if self._scraper_days > 0:
            time_threshold = datetime.now() - timedelta(days=self._scraper_days)
            logger.info(f"本次刮削将仅处理 {self._scraper_days} 天内新增的文件 (修改时间晚于 {time_threshold.strftime('%Y-%m-%d %H:%M:%S')})")

        # 排除目录
        exclude_paths = self._exclude_paths.split("\n")
        # 已选择的目录
        paths = self._scraper_paths.split("\n")
        # 需要适削的媒体文件夹
        scraper_paths = []
        for path in paths:
            if not path:
                continue
            # 强制指定该路径媒体类型
            mtype = None
            if str(path).count("#") == 1:
                mtype = next(
                    (mediaType for mediaType in MediaType.__members__.values() if
                     mediaType.value == str(str(path).split("#")[1])),
                    None)
                path = str(path).split("#")[0]
            # 判断路径是否存在
            scraper_path = Path(path)
            if not scraper_path.exists():
                logger.warning(f"MP媒体库刮削路径不存在：{path}")
                continue
            logger.info(f"开始检索目录：{path} {mtype} ...")
            # 遍历所有文件
            files = SystemUtils.list_files(scraper_path, settings.RMT_MEDIAEXT)
            for file_path in files:
                if self._event.is_set():
                    logger.info(f"MP媒体库刮削服务停止")
                    return

                # 【新增】根据时间阈值筛选文件
                if time_threshold:
                    try:
                        # 获取文件最后修改时间
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        # 如果文件修改时间早于阈值，则跳过
                        if file_mtime < time_threshold:
                            logger.debug(f"{file_path} 修改时间 {file_mtime.strftime('%Y-%m-%d')}，早于阈值，跳过...")
                            continue
                    except Exception as e:
                        logger.error(f"获取文件 {file_path} 修改时间失败: {e}")
                        continue

                # 排除目录
                exclude_flag = False
                for exclude_path in exclude_paths:
                    try:
                        if file_path.is_relative_to(Path(exclude_path)):
                            exclude_flag = True
                            break
                    except Exception as err:
                        print(str(err))
                if exclude_flag:
                    logger.debug(f"{file_path} 在排除目录中，跳过 ...")
                    continue
                # 识别是电影还是电视剧
                if not mtype:
                    file_meta = MetaInfoPath(file_path)
                    mtype = file_meta.type
                # 重命名格式
                rename_format = settings.TV_RENAME_FORMAT \
                    if mtype == MediaType.TV else settings.MOVIE_RENAME_FORMAT
                # 计算重命名中的文件夹层数
                rename_format_level = len(rename_format.split("/")) - 1
                if rename_format_level < 1:
                    continue
                # 取相对路径的第1层目录
                media_path = file_path.parents[rename_format_level - 1]
                dir_item = (media_path, mtype)
                if dir_item not in scraper_paths:
                    logger.info(f"发现目录：{dir_item}")
                    scraper_paths.append(dir_item)
        # 开始刮削
        if scraper_paths:
            for item in scraper_paths:
                logger.info(f"开始刮削目录：{item[0]} ...")
                self.__scrape_dir(path=item[0], mtype=item[1])
        else:
            logger.info(f"未发现需要刮削的目录")

    def __scrape_dir(self, path: Path, mtype: MediaType):
        """
        削刮一个目录，该目录必须是媒体文件目录
        """
        # 优先读取本地nfo文件
        tmdbid = None
        if mtype == MediaType.MOVIE:
            # 电影
            movie_nfo = path / "movie.nfo"
            if movie_nfo.exists():
                tmdbid = self.__get_tmdbid_from_nfo(movie_nfo)
            file_nfo = path / (path.stem + ".nfo")
            if not tmdbid and file_nfo.exists():
                tmdbid = self.__get_tmdbid_from_nfo(file_nfo)
        else:
            # 电视剧
            tv_nfo = path / "tvshow.nfo"
            if tv_nfo.exists():
                tmdbid = self.__get_tmdbid_from_nfo(tv_nfo)
        if tmdbid:
            # 按TMDBID识别
            logger.info(f"读取到本地nfo文件的tmdbid：{tmdbid}")
            mediainfo = self.chain.recognize_media(tmdbid=tmdbid, mtype=mtype)
        else:
            # 按名称识别
            meta = MetaInfoPath(path)
            meta.type = mtype
            mediainfo = self.chain.recognize_media(meta=meta)
        if not mediainfo:
            logger.warn(f"未识别到媒体信息：{path}")
            return

        # 如果未开启新增已入库媒体是否跟随TMDB信息变化则根据tmdbid查询之前的title
        if not settings.SCRAP_FOLLOW_TMDB:
            transfer_history = TransferHistoryOper().get_by_type_tmdbid(tmdbid=mediainfo.tmdb_id,
                                                                        mtype=mediainfo.type.value)
            if transfer_history:
                mediainfo.title = transfer_history.title
        # 获取图片
        self.chain.obtain_images(mediainfo)
        # 刮削
        MediaChain().scrape_metadata(
            fileitem=schemas.FileItem(
                storage="local",
                type="dir",
                path=str(path).replace("\\", "/") + "/",
                name=path.name,
                basename=path.stem,
                modify_time=path.stat().st_mtime,
            ),
            mediainfo=mediainfo,
            overwrite=True if self._mode else False
        )
        logger.info(f"{path} 刮削完成")

    @staticmethod
    def __get_tmdbid_from_nfo(file_path: Path):
        """
        从nfo文件中获取信息
        :param file_path:
        :return: tmdbid
        """
        if not file_path:
            return None
        xpaths = [
            "uniqueid[@type='Tmdb']",
            "uniqueid[@type='tmdb']",
            "uniqueid[@type='TMDB']",
            "tmdbid"
        ]
        try:
            reader = NfoReader(file_path)
            for xpath in xpaths:
                tmdbid = reader.get_element_value(xpath)
                if tmdbid:
                    return tmdbid
        except Exception as err:
            logger.warn(f"从nfo文件中获取tmdbid失败：{str(err)}")
        return None

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))