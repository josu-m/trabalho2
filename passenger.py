"""
passenger.py  –  Passageiro (Processo Cliente)
───────────────────────────────────────────────
Cada passageiro é um processo independente que:
  1. Chega ao aeroporto no momento definido (simula com sleep)
  2. Regista-se no sistema (memória partilhada)
  3. Entra na fila de embarque com a sua prioridade
  4. Espera até ser atendido ou desistir
"""

import time
import random

from shared_memory import make_passenger, MAX_WAIT
from logger import setup_logger

logger = setup_logger("airport")


def passenger_process(pid: int, name: str, ticket_class: str,
                       delay: float,
                       passengers,      # Manager.dict partilhado
                       queue):          # SharedPriorityQueue partilhada
    """
    Função executada por cada processo de passageiro.

    pid          – identificador único
    name         – nome do passageiro
    ticket_class – "primeira" | "executiva" | "economica"
    delay        – segundos até chegar ao aeroporto (desde o início da simulação)
    passengers   – dicionário partilhado com dados de todos os passageiros
    queue        – fila de prioridade partilhada
    """

    # ── 1. Simula viagem até ao aeroporto ──
    time.sleep(delay)

    arrival = time.time()
    p = make_passenger(pid, name, ticket_class, arrival)
    passengers[pid] = p

    logger.info(
        f"🚶 Chegada | {name} (ID {pid}) | Classe: {ticket_class.capitalize()} "
        f"| Prioridade: {p['priority']}"
    )

    # ── 2. Entra na fila de embarque ──
    queue.push(p)

    # ── 3. Aguarda até embarcar ou desistir ──
    while True:
        time.sleep(0.5)
        current = passengers.get(pid)
        if current is None:
            break
        status = current["status"]
        if status in ("boarding", "boarded", "abandoned"):
            break
        # Verifica se ultrapassou o limite (fallback caso o servidor esteja ocupado)
        if time.time() - arrival > MAX_WAIT + 2:
            break