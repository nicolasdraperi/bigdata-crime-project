import argparse
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_DEFAULT = PROJECT_ROOT / "data" / "raw" / "crime" / "crime_raw.csv"


def run_in_namenode(cmd_in_container: list[str]) -> None:
    """
    Exécute une commande à l'intérieur du conteneur 'namenode'
    via `docker compose exec`.
    """
    full_cmd = ["docker", "compose", "exec", "-T", "namenode"] + cmd_in_container
    result = subprocess.run(full_cmd, text=True, capture_output=True)

    if result.returncode != 0:
        print(f"Commande échouée : {' '.join(full_cmd)}")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        raise SystemExit(result.returncode)


def upload_to_hdfs(local_path: Path, hdfs_dir: str, overwrite: bool = False) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"Fichier local introuvable : {local_path}")

    # 1) créer le dossier cible dans HDFS
    run_in_namenode(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])

    # 2) chemin du fichier côté conteneur (volume ./data -> /data)
    container_local_path = f"/data/raw/crime/{local_path.name}"
    hdfs_target = f"{hdfs_dir}/{local_path.name}"

    put_cmd = ["hdfs", "dfs", "-put"]
    if overwrite:
        put_cmd.append("-f")
    put_cmd.extend([container_local_path, hdfs_target])

    print(f"Envoi de {local_path} vers HDFS : {hdfs_target}")
    run_in_namenode(put_cmd)
    print("Terminé ✅")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Charge le CSV de crimes depuis le système de fichiers local vers HDFS (dans le conteneur namenode)."
    )
    parser.add_argument(
        "--local-path",
        type=str,
        default=str(LOCAL_DEFAULT),
        help="Chemin local du fichier CSV (sur ta machine).",
    )
    parser.add_argument(
        "--hdfs-dir",
        type=str,
        default="/datalake/raw/crime",
        help="Répertoire HDFS de destination.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remplace le fichier dans HDFS s'il existe déjà.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    local_path = Path(args.local_path)
    upload_to_hdfs(local_path, args.hdfs_dir, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
