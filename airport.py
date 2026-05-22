"""
airport.py  –  Aeroporto (Processo Servidor)
─────────────────────────────────────────────
Responsabilidades:
  • Receber chegadas de passageiros (via fila partilhada)
  • Gerir a fila de embarque por prioridade
  • Alocar portões e agentes de embarque
  • Detectar desistências por tempo de espera excessivo
  • Registar todas as operações no log
"""


import time
import threading

from shared_memory import (
    NUM_GATES, NUM_AGENTS, MAX_WAIT, BOARDING_TIME,
    SharedPriorityQueue,
)
from logger import setup_logger

logger = setup_logger("airport")


# ──────────────────────────────────────────────────────────
# Classe principal do servidor
# ──────────────────────────────────────────────────────────
class Airport:
    """
    Processo servidor do aeroporto.

    Parâmetros partilhados (criados pelo Manager do main):
        queue       – SharedPriorityQueue com os passageiros à espera
        passengers  – Manager.dict com todos os dados dos passageiros
        gate_sem    – Semaphore(NUM_GATES)  – controla portões disponíveis
        agent_sem   – Semaphore(NUM_AGENTS) – controla agentes disponíveis
        gates_lock  – Lock para acesso à lista de portões
        gates_state – Manager.list com estado de cada portão (True=livre)
        agents_lock – Lock para acesso à lista de agentes
        agents_state– Manager.list com estado de cada agente (True=livre)
        stop_flag   – Manager.Value('b', False) – sinal de paragem
        stats       – Manager.dict com estatísticas globais
    """

    def __init__(self, queue, passengers, gate_sem, agent_sem,
                 gates_lock, gates_state, agents_lock, agents_state,
                 stop_flag, stats):
        self.queue        = queue
        self.passengers   = passengers
        self.gate_sem     = gate_sem
        self.agent_sem    = agent_sem
        self.gates_lock   = gates_lock
        self.gates_state  = gates_state
        self.agents_lock  = agents_lock
        self.agents_state = agents_state
        self.stop_flag    = stop_flag
        self.stats        = stats

    # ── Utilitários de alocação ──────────────────────────
    def _alloc_gate(self) -> int:
        """Reserva um portão livre; devolve o seu índice (1-based)."""
        with self.gates_lock:
            for i, free in enumerate(self.gates_state):
                if free:
                    self.gates_state[i] = False
                    return i + 1
        return -1  # nunca deve acontecer após gate_sem.acquire()

    def _free_gate(self, gate_id: int):
        with self.gates_lock:
            self.gates_state[gate_id - 1] = True

    def _alloc_agent(self) -> int:
        with self.agents_lock:
            for i, free in enumerate(self.agents_state):
                if free:
                    self.agents_state[i] = False
                    return i + 1
        return -1

    def _free_agent(self, agent_id: int):
        with self.agents_lock:
            self.agents_state[agent_id - 1] = True

    # ── Verificação de desistências ──────────────────────
    def _check_abandonments(self):
        """
        Percorre todos os passageiros em espera e marca como 'abandoned'
        aqueles que ultrapassaram MAX_WAIT segundos.
        Executado periodicamente numa thread auxiliar.
        """
        while not self.stop_flag.value:
            now = time.time()
            for pid, p in list(self.passengers.items()):
                if p["status"] == "waiting":
                    waited = now - p["arrival_time"]
                    if waited > MAX_WAIT:
                        p = dict(p)           # cópia mutável
                        p["status"] = "abandoned"
                        p["wait_time"] = round(waited, 2)
                        self.passengers[pid] = p
                        self.queue.remove(pid)
                        # atualiza estatísticas
                        s = dict(self.stats)
                        s["abandoned"] += 1
                        self.stats.update(s)
                        logger.warning(
                            f"❌ Passageiro {p['name']} (ID {pid}) DESISTIU "
                            f"após {waited:.1f}s de espera."
                        )
            time.sleep(1)

    # ── Embarque de um único passageiro (thread) ─────────
    def _board_passenger(self, pid: int, gate_id: int, agent_id: int):
        """
        Simula o processo de embarque de um passageiro.
        Liberta o portão e o agente no final.
        """
        p = dict(self.passengers.get(pid, {}))
        if not p or p["status"] == "abandoned":
            # Passageiro desistiu entretanto
            self._free_gate(gate_id)
            self._free_agent(agent_id)
            self.gate_sem.release()
            self.agent_sem.release()
            return

        now = time.time()
        wait = round(now - p["arrival_time"], 2)
        duration = BOARDING_TIME[p["priority"]]

        p["status"]        = "boarding"
        p["wait_time"]     = wait
        p["gate"]          = gate_id
        p["agent"]         = agent_id
        p["boarding_start"] = now
        self.passengers[pid] = p

        logger.info(
            f"✈  Embarque INICIADO | {p['name']} | Classe: {p['ticket_class'].capitalize()} "
            f"| Portão {gate_id} | Agente {agent_id} | Espera: {wait}s"
        )

        # Simula duração do embarque
        time.sleep(duration)

        p = dict(self.passengers[pid])
        p["status"]        = "boarded"
        p["boarding_time"] = duration
        self.passengers[pid] = p

        logger.info(
            f"✅ Embarque CONCLUÍDO | {p['name']} | Portão {gate_id} "
            f"| Duração: {duration}s"
        )

        # Atualiza estatísticas
        s = dict(self.stats)
        s["boarded"]    += 1
        s["total_wait"] += wait
        self.stats.update(s)

        # Liberta recursos
        self._free_gate(gate_id)
        self._free_agent(agent_id)
        self.gate_sem.release()
        self.agent_sem.release()

    # ── Loop principal do servidor ───────────────────────
    def run(self):
        logger.info("=" * 60)
        logger.info("🛫 AEROPORTO INICIADO")
        logger.info(f"   Portões: {NUM_GATES}  |  Agentes: {NUM_AGENTS}  |  Espera máx.: {MAX_WAIT}s")
        logger.info("=" * 60)

        # Thread para detectar desistências em background
        abandon_thread = threading.Thread(
            target=self._check_abandonments, daemon=True
        )
        abandon_thread.start()

        active_threads = []

        while not self.stop_flag.value:
            # Tenta obter próximo passageiro da fila
            entry = self.queue.pop()
            if entry is None:
                time.sleep(0.2)
                continue

            _prio, _arrival, pid = entry
            p = self.passengers.get(pid)
            if p is None or p["status"] != "waiting":
                continue  # passageiro desistiu entretanto

            # Aguarda portão e agente disponíveis (com timeout para não bloquear)
            got_gate  = self.gate_sem.acquire(timeout=1)
            if not got_gate:
                # Recoloca na fila e tenta mais tarde
                self.queue.push(p)
                time.sleep(0.1)
                continue

            got_agent = self.agent_sem.acquire(timeout=1)
            if not got_agent:
                self.gate_sem.release()
                self.queue.push(p)
                time.sleep(0.1)
                continue

            gate_id  = self._alloc_gate()
            agent_id = self._alloc_agent()

            # Lança thread de embarque
            t = threading.Thread(
                target=self._board_passenger,
                args=(pid, gate_id, agent_id),
                daemon=True
            )
            t.start()
            active_threads.append(t)

            # Limpa threads já terminadas
            active_threads = [th for th in active_threads if th.is_alive()]

        # Aguarda todos os embarques em curso terminarem
        for t in active_threads:
            t.join()

        self._print_summary()

    # ── Sumário final ────────────────────────────────────
    def _print_summary(self):
        s = dict(self.stats)
        boarded   = s.get("boarded", 0)
        abandoned = s.get("abandoned", 0)
        total     = boarded + abandoned
        avg_wait  = (s.get("total_wait", 0) / boarded) if boarded else 0

        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 SUMÁRIO FINAL DO DIA")
        logger.info(f"   Total de passageiros : {total}")
        logger.info(f"   Embarcados           : {boarded}")
        logger.info(f"   Desistências         : {abandoned}")
        logger.info(f"   Tempo médio de espera: {avg_wait:.2f}s")
        logger.info("=" * 60)


# ── Função de entrada (usada por multiprocessing.Process) ──
def run_airport(queue, passengers, gate_sem, agent_sem,
                gates_lock, gates_state, agents_lock, agents_state,
                stop_flag, stats):
    airport = Airport(queue, passengers, gate_sem, agent_sem,
                      gates_lock, gates_state, agents_lock, agents_state,
                      stop_flag, stats)
    airport.run()