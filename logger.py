"""
logger.py
Módulo de registo (log) de operações do sistema de embarque.
Escreve para ficheiro e para o ecrã de forma thread-safe.
Compatível com Windows (sem bloqueio de ficheiro entre processos).
"""
 
import logging
import logging.handlers
import sys
from shared_memory import LOG_FILE
 
 
def setup_logger(name: str = "airport") -> logging.Logger:
    """
    Configura e devolve um logger que escreve simultaneamente
    para o ficheiro LOG_FILE e para stdout.
    Usa RotatingFileHandler com delay=True para compatibilidade Windows.
    """
    logger = logging.getLogger(name)
    if logger.handlers:          # evita duplicação se chamado várias vezes
        return logger
 
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S"
    )
 
    # Handler para ficheiro — abre e fecha a cada escrita (compatível Windows)
    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8", delay=False)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
 
    # Handler para consola
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
 
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
