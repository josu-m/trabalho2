"""
main.py  –  Ponto de entrada da simulação
──────────────────────────────────────────
Compatível com Windows (multiprocessing spawn) e Linux/Mac (fork).
 
Uso:
    python main.py normal   – chegadas espaçadas (padrão)
    python main.py surge    – surto de passageiros em simultâneo
"""
 
import multiprocessing
import time
import os
import sys
 
from shared_memory import NUM_GATES, NUM_AGENTS, MAX_WAIT, SharedPriorityQueue
from airport import run_airport
from passenger import passenger_process
from logger import setup_logger
 
# ──────────────────────────────────────────────────────────
# Dados de passageiros para a simulação
# ──────────────────────────────────────────────────────────
PASSENGER_DATA = [
    # (nome,              classe,      delay_chegada em segundos)
    ("Ana Silva",        "primeira",   1),
    ("Bruno Costa",      "economica",  2),
    ("Carla Mendes",     "executiva",  2),
    ("David Santos",     "economica",  3),
    ("Eva Ferreira",     "primeira",   4),
    ("Francisco Lima",   "executiva",  5),
    ("Gabriela Rocha",   "economica",  5),
    ("Hugo Martins",     "economica",  6),
    ("Inês Carvalho",    "primeira",   7),
    ("João Oliveira",    "economica",  8),
    ("Karina Sousa",     "executiva",  9),
    ("Luís Pinto",       "economica", 10),
]
 
# Cenário de surto: todos chegam quase ao mesmo tempo
SURGE_DATA = [
    ("Surge_P1",  "economica", 0.1),
    ("Surge_P2",  "economica", 0.1),
    ("Surge_P3",  "executiva", 0.1),
    ("Surge_P4",  "primeira",  0.1),
    ("Surge_P5",  "economica", 0.2),
    ("Surge_P6",  "economica", 0.2),
    ("Surge_P7",  "executiva", 0.3),
    ("Surge_P8",  "economica", 0.3),
]
 
 
# ──────────────────────────────────────────────────────────
# Função principal
# ──────────────────────────────────────────────────────────
def main(scenario: str = "normal"):
    logger = setup_logger("airport")
    logger.info(f"🌍 Cenário seleccionado: {scenario.upper()}")
 
    # ── Memória partilhada via Manager ──────────────────
    with multiprocessing.Manager() as manager:
 
        # Dicionário partilhado com dados de todos os passageiros
        passengers = manager.dict()
 
        # Fila de prioridade partilhada
        queue = SharedPriorityQueue(manager)
 
        # Semáforos de recursos
        gate_sem  = manager.Semaphore(NUM_GATES)
        agent_sem = manager.Semaphore(NUM_AGENTS)
 
        # Estado individual dos portões e agentes (True = livre)
        gates_lock   = manager.Lock()
        gates_state  = manager.list([True] * NUM_GATES)
        agents_lock  = manager.Lock()
        agents_state = manager.list([True] * NUM_AGENTS)
 
        # Sinal de paragem para o servidor
        stop_flag = manager.Value('b', False)
 
        # Estatísticas globais
        stats = manager.dict({"boarded": 0, "abandoned": 0, "total_wait": 0.0})
 
        # ── Inicia servidor (Aeroporto) ──────────────────
        server = multiprocessing.Process(
            target=run_airport,
            args=(queue, passengers, gate_sem, agent_sem,
                  gates_lock, gates_state, agents_lock, agents_state,
                  stop_flag, stats),
            name="Aeroporto"
        )
        server.start()
        logger.info(f"🖥  Servidor iniciado (PID {server.pid})")
 
        # ── Define lista de passageiros consoante cenário ─
        data = SURGE_DATA if scenario == "surge" else PASSENGER_DATA
 
        # ── Lança processos de passageiros ───────────────
        procs = []
        for idx, (name, ticket_class, delay) in enumerate(data, start=1):
            p = multiprocessing.Process(
                target=passenger_process,
                args=(idx, name, ticket_class, delay, passengers, queue),
                name=f"Passageiro-{idx}"
            )
            p.start()
            procs.append(p)
 
        logger.info(f"👥 {len(procs)} passageiros lançados.")
 
        # ── Aguarda todos os passageiros terminarem ───────
        for p in procs:
            p.join()
 
        logger.info("⏳ Todos os passageiros terminaram. A aguardar embarques em curso...")
 
        # Dá tempo para o servidor concluir os embarques em curso
        time.sleep(NUM_GATES * 6)
 
        # ── Sinaliza paragem do servidor ──────────────────
        stop_flag.value = True
        server.join(timeout=10)
        if server.is_alive():
            server.terminate()
 
        logger.info("🏁 Simulação concluída. Consulte 'airport.log' para o registo completo.")
 
 
# ──────────────────────────────────────────────────────────
# IMPORTANTE: no Windows o multiprocessing usa "spawn",
# por isso TODA a lógica de arranque deve estar dentro
# do bloco if __name__ == "__main__"
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Necessário no Windows para multiprocessing funcionar correctamente
    multiprocessing.freeze_support()
 
    # Limpa o log anterior de forma segura (sem apagar o ficheiro)
    try:
        with open("airport.log", "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass  # se falhar, o log é simplesmente acrescentado
 
    scenario = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if scenario not in ("normal", "surge"):
        print("Uso: python main.py [normal|surge]")
        sys.exit(1)
 
    main(scenario)
