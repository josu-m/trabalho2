"""
shared_memory.py
Módulo de memória partilhada e estruturas de dados comuns.
Define as estruturas partilhadas entre o servidor (aeroporto) e os clientes (passageiros).
"""

import multiprocessing
import heapq
import time
from dataclasses import dataclass, field
from typing import Optional

# ──────────────────────────────────────────────
# Constantes de configuração do sistema
# ──────────────────────────────────────────────
NUM_GATES   = 3          # Número de portões de embarque
NUM_AGENTS  = 4          # Número de agentes de embarque disponíveis
MAX_WAIT    = 30         # Tempo máximo de espera (segundos) antes de desistência
LOG_FILE    = "airport.log"

# Mapeamento de classe de bilhete → prioridade numérica (menor = maior prioridade)
PRIORITY_MAP = {
    "primeira":   1,
    "executiva":  2,
    "economica":  3,
}

# Tempo de embarque (segundos) por prioridade
BOARDING_TIME = {
    1: 2,   # primeira classe – rápido
    2: 3,   # executiva
    3: 5,   # económica – mais demorado
}


# ──────────────────────────────────────────────
# Estrutura de um passageiro (partilhável via Manager)
# ──────────────────────────────────────────────
def make_passenger(pid: int, name: str, ticket_class: str, arrival_time: float) -> dict:
    """Cria um dicionário com os dados de um passageiro."""
    return {
        "pid":           pid,
        "name":          name,
        "ticket_class":  ticket_class,
        "priority":      PRIORITY_MAP[ticket_class],
        "arrival_time":  arrival_time,
        "wait_time":     None,    # preenchido ao iniciar embarque
        "boarding_time": None,    # preenchido ao concluir embarque
        "gate":          None,
        "agent":         None,
        "status":        "waiting",  # waiting | boarding | boarded | abandoned
    }


# ──────────────────────────────────────────────
# Fila de prioridade thread-safe via Manager.list + Lock
# ──────────────────────────────────────────────
class SharedPriorityQueue:
    """
    Fila de prioridade baseada em heap, protegida por semáforo.
    Armazenada numa Manager.list para partilha entre processos.
    Cada entrada é (priority, arrival_time, passenger_id).
    """

    def __init__(self, manager):
        self._heap  = manager.list()   # lista partilhada (representa o heap)
        self._lock  = manager.Lock()   # exclusão mútua

    def push(self, passenger: dict):
        with self._lock:
            heap = list(self._heap)
            heapq.heappush(heap, (passenger["priority"],
                                  passenger["arrival_time"],
                                  passenger["pid"]))
            self._heap[:] = heap

    def pop(self) -> Optional[tuple]:
        """Retorna (priority, arrival_time, pid) ou None se vazia."""
        with self._lock:
            heap = list(self._heap)
            if not heap:
                return None
            item = heapq.heappop(heap)
            self._heap[:] = heap
            return item

    def remove(self, pid: int):
        """Remove passageiro da fila (desistência)."""
        with self._lock:
            heap = [e for e in self._heap if e[2] != pid]
            heapq.heapify(heap)
            self._heap[:] = heap

    def snapshot(self) -> list:
        """Devolve cópia actual da fila ordenada."""
        with self._lock:
            return sorted(list(self._heap))

    def __len__(self):
        return len(self._heap)
