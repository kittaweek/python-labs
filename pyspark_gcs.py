import os
import sys

sys.path.insert(0, os.path.abspath("../"))

import logging
from pathlib import Path
from typing import Optional, Union
from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
project_path = Path().resolve().parent


def _get_spark_jars_path() -> Path:
    if "SPARK_HOME" in os.environ:
        logger.debug(
            f"Using SPARK_HOME {os.environ['SPARK_HOME']} to find jars directory"
        )
        jars = Path(os.environ["SPARK_HOME"]).joinpath("jars")
    else:
        logger.debug("Using pyspark package to find jars directory")
        import pyspark

        jars = Path(pyspark.__file__).parent.joinpath("jars")

    if not jars.exists():
        raise ValueError(
            f"Can't find Spark jars, my best guess was: `{jars.as_posix()}`"
            f", you can use `SPARK_HOME` to help out!"
        )
    return jars


def _connector_already_installed(jars: Path) -> bool:
    if len(list(jars.glob("*gcs-connector*"))) > 0:
        return True
    return False


def _find_major_version_of_hadoop(jars: Path) -> int:
    return 2 if len(list(jars.glob("hadoop-common-2*"))) else 3


def get_gcs_enabled_config(
    project: Optional[str] = None,
    email: Optional[str] = None,
    jars_dir: Optional[Path] = None,
    conf: Optional[SparkConf] = None,
    service_account_keyfile_path: Optional[str] = None,
) -> SparkConf:
    """Returns GCS enabled SparkConf object, which you can use to create Session/Context"""
    jconf = conf._jconf if conf else None  # type:ignore[attr-defined]
    conf = SparkConf(_jconf=jconf).set(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    if project:
        conf.set("spark.hadoop.fs.gs.project.id", project)

    if email:
        conf.set("spark.hadoop.fs.gs.auth.service.account.email", email)

    if str(service_account_keyfile_path).endswith(".json"):
        conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true").set(
            "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
            service_account_keyfile_path,
        )
    elif str(service_account_keyfile_path).endswith(".p12"):
        conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true").set(
            "spark.hadoop.fs.gs.auth.service.account.keyfile",
            service_account_keyfile_path,
        )

    jars = jars_dir or _get_spark_jars_path()
    if not _connector_already_installed(jars):
        hadoop_major = _find_major_version_of_hadoop(jars)
        # our setup.cfg says that this package should not be installed as zip
        # so we should be able to operate on files like FS to find the
        # gcs jar file:
        if hadoop_major == 2:
            raise ValueError("Hadoop 2 not supported anymore")
        else:
            logger.debug("Inferred Hadoop 3, using hadoop3 GCS connector jar")
            gcs_connector_jar = project_path.joinpath(
                "jars", "gcs-connector-hadoop2-latest.jar"
            )
        assert gcs_connector_jar.exists(), (
            "There is something wrong with the pyspark_gcs installation, "
            f"can't find {gcs_connector_jar.as_posix()}"
        )
        conf.set("spark.jars", gcs_connector_jar.as_posix())

    return conf


def get_spark_gcs_session(
    service_account_keyfile_path: Union[str, Path, None] = None,
    project: Optional[str] = None,
    email: Optional[str] = None,
    conf: Optional[SparkConf] = None,
) -> SparkSession:
    """
    Returns GCS enabled SparkSession. You may specify `service_account_keyfile_path`,
    `project`, and/or extra Spark config in the `conf` parameter.
    """
    service_account_keyfile_path = project_path.joinpath(service_account_keyfile_path)
    sa_keyfile = (
        Path(service_account_keyfile_path) if service_account_keyfile_path else None
    )
    if sa_keyfile and not sa_keyfile.exists():
        raise ValueError(
            f"Service Account keyfile {service_account_keyfile_path} does not exist"
        )
    return SparkSession.builder.config(
        conf=get_gcs_enabled_config(
            project=project,
            email=email,
            conf=conf,
            service_account_keyfile_path=service_account_keyfile_path,
        ),
    ).getOrCreate()
