import streamlit as st
import pandas as pd
import os
import subprocess
import shutil
import time
from datetime import datetime
import pytz
import re, json
import fcntl

# Global lock files
EXECUTION_LOCK_FILE = "info/execution.lock"
LAST_GPU_REFRESH_FILE = "info/last_gpu_refresh_time.txt"
LAST_INFO_REFRESH_FILE = "info/last_info_refresh_time.txt"

# Cooldown settings
GPU_REFRESH_COOLDOWN_SECONDS = 5 * 60  # 5분
INFO_REFRESH_COOLDOWN_SECONDS = 24 * 60 * 60  # 1일


def get_global_last_refresh_time(refresh_type="gpu"):
    """Read the global last refresh timestamp from file."""
    file_path = LAST_GPU_REFRESH_FILE if refresh_type == "gpu" else LAST_INFO_REFRESH_FILE
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return float(f.read().strip())
        except (ValueError, IOError):
            return 0.0
    return 0.0


def set_global_last_refresh_time(timestamp, refresh_type="gpu"):
    """Write the global last refresh timestamp to file."""
    file_path = LAST_GPU_REFRESH_FILE if refresh_type == "gpu" else LAST_INFO_REFRESH_FILE
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(str(timestamp))


def acquire_execution_lock():
    """
    Try to acquire exclusive execution lock.
    Returns (lock_file, success) tuple.
    If success is True, caller must call release_execution_lock(lock_file) when done.
    """
    os.makedirs(os.path.dirname(EXECUTION_LOCK_FILE), exist_ok=True)
    lock_file = open(EXECUTION_LOCK_FILE, "w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file, True
    except (IOError, OSError):
        lock_file.close()
        return None, False


def release_execution_lock(lock_file):
    """Release the execution lock."""
    if lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
        except (IOError, OSError):
            pass


def get_ansible_playbook_path():
    """
    ansible-playbook 실행 파일 경로를 찾습니다.
    여러 가능한 경로를 순서대로 확인합니다.
    """
    # 1. shutil.which로 PATH에서 찾기
    ansible_path = shutil.which("ansible-playbook")
    if ansible_path:
        return ansible_path
    
    # 2. 일반적인 설치 경로들 확인
    common_paths = [
        "/usr/bin/ansible-playbook",
        "/usr/local/bin/ansible-playbook",
        os.path.expanduser("~/.local/bin/ansible-playbook"),
        os.path.expanduser("~/anaconda3/bin/ansible-playbook"),
        os.path.expanduser("~/miniconda3/bin/ansible-playbook"),
        "/opt/anaconda3/bin/ansible-playbook",
    ]
    
    for path in common_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    
    # 3. 찾지 못한 경우 None 반환
    return None


def get_hostname_mapping():
    return {
        'snu185': '147.46.92.185',
        'snu30': '147.46.91.30',
        'snu188': '147.46.92.188',
        'snu32': '147.46.91.32',
        'snu35': '147.46.91.35',
        'snu20': '147.46.91.20',
        'snu36': '147.46.91.36',
        'snu186': '147.46.92.186',
        'snu44': '147.46.92.44',
        'snu24': '147.46.91.24',
        'snu22': '147.46.91.22',
        'snu55' : '147.46.91.55',
        'nm87': '147.46.132.87',
        'nm20': '147.47.132.20',
        'nm80': '147.46.132.80',
        'info107': '147.47.206.107',
        'info100': '147.47.206.100',
        'info103': '147.47.206.103',
        'info104': '147.47.206.104',
        'info106' : '147.47.206.106',
        'info105' : '147.47.206.105',
        'snu234' : '147.47.190.234',
        'snu233' : '147.47.190.233'
    }
    
    
def get_owner_mapping():
    return {
        'snu185': '공용',
        'snu30': '공용',
        'snu188': '태언',
        'snu32': '성환',
        'snu35': '지수',
        'snu20': '지환/공용',
        'snu36': '재석/공용',
        'snu186': '공용',
        'snu44': '공용',
        'snu24': '주현',
        'snu22': '공용',
        'snu55': '현웅',
        'nm87': '희웅',
        'nm20': '진',
        'nm80': '용진',
        'info107': '지수',
        'info100': '브레인',
        'info103': '용호',
        'info104': '공용',
        'info106': '수민',
        'info105': '진우',
        'snu234': '공용',
        'snu233': '공용',
    }

def get_place_mapping():
    return {
        'snu185': '신양',
        'snu30': '신양',
        'snu188': '신양',
        'snu32': '신양',
        'snu35': '신양',
        'snu20': '신양',
        'snu36': '신양',
        'snu186': '신양',
        'snu44': '신양',
        'snu24': '신양',
        'snu22': '신양',
        'snu55': '신양',
        'nm87': '뉴미연',
        'nm20': '뉴미연',
        'nm80': '뉴미연',
        'info107': '정보화',
        'info100': '정보화',
        'info103': '정보화',
        'info104': '정보화',
        'info106': '정보화',
        'info105': '정보화',
        'snu234': '303',
        'snu233': '303',
    }
    
    
def parse_ansible_output(output):
    """
    Parse Ansible output to extract unreachable hosts and other stats.
    """
    unreachable = []
    failed = []
    success = []

    for line in output.splitlines():
        match = re.search(r'(.*) : ok=(\d+) +changed=(\d+) +unreachable=(\d+) +failed=(\d+)', line)
        if match:
            host = match.group(1).strip()
            host_ip = get_hostname_mapping().get(host, "Unknown")
            host_info = f"{host} ({host_ip})"
            unreachable_count = int(match.group(4))
            failed_count = int(match.group(5))
            if unreachable_count > 0:
                unreachable.append(host_info)
            elif failed_count > 0:
                failed.append(host_info)
            else:
                success.append(host_info)
    
    return {
        "unreachable": unreachable,
        "failed": failed,
        "success": success
    }

def load_cpu_data(directory):
    hostname_mapping = get_hostname_mapping()
    data = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            server_name = file_name.split(".")[0]
            with open(os.path.join(directory, file_name)) as f:
                server_data = f.readlines()
                first_line = server_data[0]
                load_info = first_line.split("load average: ")
                min1, min5, min15 = load_info[1].split(", ")
                data.append({
                    "server": server_name,
                    "ip": hostname_mapping.get(server_name, "Unknown"),
                    "load_avg_1min": float(min1),
                    "load_avg_5min": float(min5),
                    "load_avg_15min": float(min15)
                })
    return pd.DataFrame(data)

def load_gpu_data(directory):
    hostname_mapping = get_hostname_mapping()
    data = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            server_name = file_name.split(".")[0]
            with open(os.path.join(directory, file_name)) as f:
                for line in f:
                    gpu_info = line.strip().split(", ")
                    data.append({
                        "server": server_name,
                        "ip": hostname_mapping.get(server_name, "Unknown"),
                        "gpu_index": int(gpu_info[0]),
                        "gpu_name": gpu_info[1],
                        "memory_total": int(gpu_info[2]),
                        "memory_used": int(gpu_info[3]),
                        "utilization": int(gpu_info[4])
                    })
    return pd.DataFrame(data)

def load_os_data(directory):
    hostname_mapping = get_hostname_mapping()
    data = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            server_name = file_name.split(".")[0]
            with open(os.path.join(directory, file_name)) as f:
                OS_info = f.readlines()[0].strip()
                data.append({
                    "server": server_name,
                    "ip": hostname_mapping.get(server_name, "Unknown"),
                    "OS_info": OS_info
                })
    return pd.DataFrame(data)

def load_cuda_data(directory):
    """
    Load CUDA driver information from /usr/local/ directory.
    Each JSON file contains a list of CUDA versions installed on the server.
    Only cuda-XX.X format versions are considered valid (e.g., cuda-11.8, cuda-12.1)
    """
    hostname_mapping = get_hostname_mapping()
    data = []
    
    # cuda-XX.X 형식 매칭 (예: cuda-11.8, cuda-12.1)
    cuda_version_pattern = re.compile(r'^cuda-\d+\.\d+$')
    
    if not os.path.exists(directory):
        return pd.DataFrame(columns=["server", "ip", "cuda_versions"])
    
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            server_name = file_name.split(".")[0]
            with open(os.path.join(directory, file_name)) as f:
                content = f.read().strip()
                if content:
                    try:
                        cuda_versions = json.loads(content)
                        if isinstance(cuda_versions, list):
                            # cuda-XX.X 형식만 필터링
                            cuda_versions = [v for v in cuda_versions if cuda_version_pattern.match(v)]
                            # 버전 정렬 (cuda- 제거 후 숫자로 정렬)
                            cuda_versions = sorted(cuda_versions, key=lambda x: [int(n) for n in x.replace('cuda-', '').split('.')])
                    except json.JSONDecodeError:
                        cuda_versions = [v.strip() for v in content.replace('\n', ',').split(',') if v.strip()]
                        cuda_versions = [v for v in cuda_versions if cuda_version_pattern.match(v)]
                else:
                    cuda_versions = []
                
                data.append({
                    "server": server_name,
                    "ip": hostname_mapping.get(server_name, "Unknown"),
                    "cuda_versions": cuda_versions
                })
    
    return pd.DataFrame(data)
def make_time_format(dt):
    # KST timezone으로 포맷팅
    return dt.strftime('%Y/%m/%d-%H:%M:%S %Z')
def get_update_time(cpu_dir, gpu_dir):
    # CPU 파일들의 최신 수정 시간 확인
    cpu_times = [
        datetime.fromtimestamp(os.path.getatime(cpu_dir + "/" + file))
        .astimezone(pytz.timezone("Asia/Seoul"))
        for file in os.listdir(cpu_dir) 
        if file.endswith(".json")
    ]
    
    # GPU 파일들의 최신 수정 시간 확인
    gpu_times = [
        datetime.fromtimestamp(os.path.getatime(gpu_dir + "/" + file))
        .astimezone(pytz.timezone("Asia/Seoul"))
        for file in os.listdir(gpu_dir) 
        if file.endswith(".json")
    ]
    
    # 가장 최신 시간 찾기
    latest_time = max(max(cpu_times), max(gpu_times))
    return latest_time

def display_health_status(stats):
    """
    Display the health status based on Ansible output.
    """
    st.markdown("### Health Status")
    st.link_button("서버정보 페이지 (노션)", "https://www.notion.so/b370f0f0e94646299f133c85a2693505")
    # 링크 : https://www.notion.so/b370f0f0e94646299f133c85a2693505
    st.markdown("""
    - 🟢 정상 작동하는 서버
    - 🔴 접속 불가능한 서버
    - ⚠️ 접속은 되었으나 상태 점검에 실패한 서버
    """)
        
    failed_server_list = []
    if "failed" in stats and stats["failed"]:
        st.error(f"⚠️ 상태 점검 실패한 서버: {', '.join(stats['failed'])}")
        failed_server_list = stats['failed']
    
    if "unreachable" in stats and stats["unreachable"]:
        # st.warning("🔴 접속 불가능한 서버")
        rows = []
        for s in stats["unreachable"]:
            # 예: "info101 (147.47.206.101)"
            try:
                name, rest = s.split(" ", 1)
                ip = rest.strip("()")
                owner = get_owner_mapping().get(name, "-")
            except Exception:
                name, ip, owner = s, "-", "-"

            rows.append({
                "서버": name,
                "IP": ip,
                "담당": owner,
                "상태": "Unreachable"
            })

        df_unreachable = pd.DataFrame(rows)

        with st.expander("🔴 접속 불가능한 서버 목록 보기"):
            st.dataframe(df_unreachable, use_container_width=True)

    else:
        st.info("🟢 접속 불가능한 서버 없음")

    if "success" in stats and stats["success"]:
        # st.success("🟢 정상 작동하는 서버")

        rows = []
        for s in stats["success"]:
            # 예: "info100 (147.47.206.100)"
            try:
                name, rest = s.split(" ", 1)
                ip = rest.strip("()")
                owner = get_owner_mapping().get(name, "-")
                rows.append({"서버": name, "IP": ip, "담당": owner})
            except Exception as e:
                print(e, "on ", s)
                rows.append({"서버": s, "IP": "-", "담당": "-"})

        df = pd.DataFrame(rows)

        with st.expander("🟢 정상 작동하는 서버 목록 보기"):
            st.dataframe(df, use_container_width=True)

st.title("Server Monitor")
cpu_data_directory = "./info/cpu_status"
gpu_data_directory = "./info/gpu_status"
os_data_directory = "./info/os_status"
cuda_data_directory = "./info/cuda_status"  # 새로 추가된 CUDA 디렉토리
updated_time = get_update_time(cpu_data_directory, gpu_data_directory)
health_status_info = "info/health_status.json"
# 세션 상태를 사용해 리프레시 상태 관리
if "ansible_stats" not in st.session_state:
    if os.path.exists(health_status_info):
        with open(health_status_info) as f:
            st.session_state.ansible_stats = json.load(f)
    else:
        st.session_state.ansible_stats = {"unreachable": [], "failed": [], "success": []}

# GPU 쿨다운 확인 (5분)
gpu_last_refresh_time = get_global_last_refresh_time("gpu")
can_refresh_gpu = True
gpu_remaining_seconds = 0
if gpu_last_refresh_time > 0:
    gpu_elapsed = time.time() - gpu_last_refresh_time
    if gpu_elapsed < GPU_REFRESH_COOLDOWN_SECONDS:
        can_refresh_gpu = False
        gpu_remaining_seconds = int(GPU_REFRESH_COOLDOWN_SECONDS - gpu_elapsed)

# Info 쿨다운 확인 (1일)
info_last_refresh_time = get_global_last_refresh_time("info")
can_refresh_info = True
info_remaining_seconds = 0
if info_last_refresh_time > 0:
    info_elapsed = time.time() - info_last_refresh_time
    if info_elapsed < INFO_REFRESH_COOLDOWN_SECONDS:
        can_refresh_info = False
        info_remaining_seconds = int(INFO_REFRESH_COOLDOWN_SECONDS - info_elapsed)

print("Can Refresh GPU:", can_refresh_gpu, "Remaining:", gpu_remaining_seconds)
print("Can Refresh Info:", can_refresh_info, "Remaining:", info_remaining_seconds)

# GPU 쿨다운 메시지
if not can_refresh_gpu:
    gpu_minutes = gpu_remaining_seconds // 60
    gpu_seconds = gpu_remaining_seconds % 60
    gpu_available_at = datetime.fromtimestamp(
        gpu_last_refresh_time + GPU_REFRESH_COOLDOWN_SECONDS
    ).astimezone(pytz.timezone("Asia/Seoul"))
    st.warning(f"⏳ GPU Refresh: {gpu_minutes}분 {gpu_seconds}초 남음 (기준 {GPU_REFRESH_COOLDOWN_SECONDS//60}분, 전체 유저 공유)")

# Info 쿨다운 메시지
if not can_refresh_info:
    info_hours = info_remaining_seconds // 3600
    info_minutes = (info_remaining_seconds % 3600) // 60
    info_available_at = datetime.fromtimestamp(
        info_last_refresh_time + INFO_REFRESH_COOLDOWN_SECONDS
    ).astimezone(pytz.timezone("Asia/Seoul"))
    st.warning(f"⏳ Info Refresh: {info_hours}시간 {info_minutes}분 남음 (기준 1일, 전체 유저 공유)")

# 두 개의 버튼을 나란히 배치
col_btn1, col_btn2 = st.columns(2)

with col_btn1:
    refresh_gpu_clicked = st.button("Refresh GPU", disabled=not can_refresh_gpu)

with col_btn2:
    refresh_info_clicked = st.button("Refresh Info", disabled=not can_refresh_info)

# Refresh GPU 버튼 처리 (GPU 메모리/사용률만 업데이트, 5분 쿨다운)
if refresh_gpu_clicked:
    lock_file, lock_acquired = acquire_execution_lock()

    if not lock_acquired:
        st.error("🔒 다른 사용자가 현재 데이터를 새로고침 중입니다. 잠시 후 새로고침해서 정보를 업데이트해주세요.")
    else:
        try:
            current_gpu_time = get_global_last_refresh_time("gpu")
            current_elapsed = time.time() - current_gpu_time
            if current_gpu_time > 0 and current_elapsed < GPU_REFRESH_COOLDOWN_SECONDS:
                st.warning("⏳ 다른 사용자가 방금 GPU 데이터를 새로고침했습니다. 페이지를 새로고침해주세요.")
            else:
                set_global_last_refresh_time(time.time(), "gpu")

                with st.spinner("Refreshing GPU data..."):
                    ansible_path = get_ansible_playbook_path()

                    if ansible_path is None:
                        st.error("ansible-playbook을 찾을 수 없습니다!")
                        st.error("다음 명령어로 설치해주세요: pip install ansible")
                    else:
                        try:
                            result = subprocess.run(
                                [ansible_path, "monitor_gpu.yml", "-i", "hosts.ini"],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            st.success("GPU data refreshed successfully!")
                        except subprocess.CalledProcessError as e:
                            if not e.stdout:
                                st.error("Error refreshing GPU data!")
                                st.error(e.stderr)
                            else:
                                st.success("GPU data refreshed!")

                        updated_time = get_update_time(cpu_data_directory, gpu_data_directory)
        finally:
            release_execution_lock(lock_file)

# Refresh Info 버튼 처리 (전체 정보 업데이트: OS, CPU, GPU + Health Status, 1일 쿨다운)
if refresh_info_clicked:
    lock_file, lock_acquired = acquire_execution_lock()

    if not lock_acquired:
        st.error("🔒 다른 사용자가 현재 데이터를 새로고침 중입니다. 잠시 후 다시 시도해주세요.")
    else:
        try:
            current_info_time = get_global_last_refresh_time("info")
            current_elapsed = time.time() - current_info_time
            if current_info_time > 0 and current_elapsed < INFO_REFRESH_COOLDOWN_SECONDS:
                st.warning("⏳ 다른 사용자가 방금 Info 데이터를 새로고침했습니다. 페이지를 새로고침해주세요.")
            else:
                set_global_last_refresh_time(time.time(), "info")

                with st.spinner("Running Ansible Playbook..."):
                    ansible_path = get_ansible_playbook_path()

                    if ansible_path is None:
                        st.error("ansible-playbook을 찾을 수 없습니다!")
                        st.error("다음 명령어로 설치해주세요: pip install ansible")
                    else:
                        try:
                            result = subprocess.run(
                                [ansible_path, "moniter_status.yml", "-i", "hosts.ini"],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            st.success("Ansible Playbook executed successfully!")
                            health_status = parse_ansible_output(result.stdout)
                        except subprocess.CalledProcessError as e:
                            health_status = parse_ansible_output(e.stdout or "")
                            if not e.stdout:
                                st.error("Error running Ansible Playbook!")
                                st.error(e.stderr)

                        with open(health_status_info, "w") as f:
                            json.dump(health_status, f)
                        st.session_state.ansible_stats = health_status
                        updated_time = get_update_time(cpu_data_directory, gpu_data_directory)
        finally:
            release_execution_lock(lock_file)
# 상태 표시 영역 동적 업데이트
status_container = st.container()
with status_container:
    display_health_status(st.session_state.ansible_stats)

# CPU 데이터 로드 및 시각화
# get file update time
st.markdown(f"Updated at : {make_time_format(updated_time)}")

# 구분선
st.markdown("--------------------------------")

try:
    # 데이터 로드
    cpu_data = load_cpu_data(cpu_data_directory)
    gpu_data = load_gpu_data(gpu_data_directory)
    os_info = load_os_data(os_data_directory)
    cuda_info = load_cuda_data(cuda_data_directory)  # CUDA 데이터 로드
    
    
    place_map = get_place_mapping()
    owner_map = get_owner_mapping()

    def add_place_owner_column(df):
        df = df.copy()
        df["place"] = df["server"].map(place_map).fillna("기타")
        df["owner"] = df["server"].map(owner_map).fillna("-")
        return df



    cpu_data = add_place_owner_column(cpu_data)
    gpu_data = add_place_owner_column(gpu_data)
    os_info = add_place_owner_column(os_info)
    cuda_info = add_place_owner_column(cuda_info)  # CUDA 데이터에도 place/owner 추가
    
    CPU_COLS = ['place', 'server', 'ip', 'owner', 'load_avg_1min', 'load_avg_5min', 'load_avg_15min']
    GPU_COLS = ['place', 'server', 'ip', 'owner', 'gpu_index', 'gpu_name', 'memory_total', 'memory_used', 'utilization']
    OS_COLS = ['place', 'server', 'ip', 'owner', 'OS_info']
    CUDA_COLS = ['place', 'server', 'ip', 'owner', 'cuda_versions']
    
    cpu_data = cpu_data[CPU_COLS]
    gpu_data = gpu_data[GPU_COLS]
    os_info = os_info[OS_COLS]
    if not cuda_info.empty:
        cuda_info = cuda_info[CUDA_COLS]


    
    places = ["신양", "뉴미연", "정보화", "303", "기타"]
    tabs = st.tabs(places)

    for tab, place in zip(tabs, places):
        with tab:
            st.markdown(f"## 📍 {place}")

            cpu_p = cpu_data[cpu_data["place"] == place]
            gpu_p = gpu_data[gpu_data["place"] == place]
            os_p  = os_info[os_info["place"] == place]
            cuda_p = cuda_info[cuda_info["place"] == place] if not cuda_info.empty else pd.DataFrame()

            if cpu_p.empty and gpu_p.empty:
                st.info("해당 위치에 서버가 없습니다.")
                continue

            # ===== GPU Summary =====
            col3, col4 = st.columns([2, 2])

            with col3:
                st.markdown("### GPU Memory Usage (%)")
                gpu_memory = gpu_p.groupby("ip").agg(
                    {"memory_used": "sum", "memory_total": "sum"}
                )
                if not gpu_memory.empty:
                    st.bar_chart(gpu_memory["memory_used"] / gpu_memory["memory_total"] * 100)

            with col4:
                st.markdown("### GPU Utilization (%)")
                gpu_utilization = gpu_p.groupby("ip")["utilization"].mean()
                if not gpu_utilization.empty:
                    st.bar_chart(gpu_utilization)

            # ===== GPU Detail Table =====
            st.markdown("### GPU Memory Usage per Server")

            if not gpu_p.empty:
                gpu_p = gpu_p.copy()
                gpu_p["memory_usage (%)"] = (
                    gpu_p["memory_used"] / gpu_p["memory_total"]
                ).round(2)

                gpu_p = gpu_p.sort_values(by=["ip", "gpu_index"])
                gpu_p["ip_display"] = gpu_p["ip"]
                gpu_p.loc[gpu_p["ip_display"].duplicated(), "ip_display"] = ""
                gpu_p["owner_display"] = gpu_p["owner"]
                gpu_p.loc[gpu_p.duplicated(subset=["ip"]), "owner_display"] = ""

                gpu_display_df = gpu_p[
                    ["ip_display", "owner_display", "gpu_index", "gpu_name", "memory_usage (%)", "utilization"]
                ]

                st.dataframe(
                    gpu_display_df,
                    column_config={
                        "ip_display": st.column_config.TextColumn("IP Address"),
                        "owner_display": st.column_config.TextColumn("담당자"),
                        "gpu_index": st.column_config.TextColumn("GPU #"),
                        "gpu_name": st.column_config.TextColumn("GPU Name"),
                        "memory_usage (%)": st.column_config.ProgressColumn(
                            "GPU Memory Usage (%)",
                            min_value=0,
                            max_value=1,
                        ),
                        "utilization": st.column_config.ProgressColumn(
                            "Utilization (%)",
                            min_value=0,
                            max_value=100,
                        ),
                    },
                    use_container_width=True,
                    hide_index=True,
                )

            # ===== CPU Info =====
            st.markdown("### CPU information")
            
            if not cpu_p.empty:
                cpu_p = cpu_p.copy()
                cpu_p.sort_values(by=["ip"], inplace=True)
                st.dataframe(
                    cpu_p.drop(columns=["place", "server"]),
                    column_config={
                        # "server": st.column_config.TextColumn("Server"),
                        "ip": st.column_config.TextColumn("IP Address"),
                        "owner": st.column_config.TextColumn("담당자"),
                        "OS_info": st.column_config.TextColumn("OS Info"),
                        "load_avg_1min": st.column_config.ProgressColumn(
                            "Load Avg (1 min)", min_value=0, max_value=100, format="%.0f%%"

                        ),
                        "load_avg_5min": st.column_config.ProgressColumn(
                            "Load Avg (5 min)", min_value=0, max_value=100, format="%.0f%%"
                        ),
                        "load_avg_15min": st.column_config.ProgressColumn(
                            "Load Avg (15 min)", min_value=0, max_value=100, format="%.0f%%"
                        ),
                    },
                    use_container_width=True,
                    hide_index=True,
                )

            # ===== OS Info =====
            st.markdown("### OS information")
            if not os_p.empty:
                os_p = os_p.copy()
                os_p.sort_values(by=["ip"], inplace=True)
                st.dataframe(
                    os_p.drop(columns=["place", "server"]),
                    column_config={
                        "ip": st.column_config.TextColumn("IP Address"),
                        "owner": st.column_config.TextColumn("담당자"),
                        "OS_info": st.column_config.TextColumn("OS Info"),
                    },
                    use_container_width=True,
                    hide_index=True,
                )

            # ===== CUDA Info =====
            st.markdown("### CUDA Drivers (/usr/local/)")
            if not cuda_p.empty:
                cuda_p = cuda_p.copy()
                cuda_p.sort_values(by=["ip"], inplace=True)
                
                # cuda_versions 리스트를 문자열로 변환하여 표시
                cuda_display = cuda_p.copy()
                cuda_display["cuda_versions_str"] = cuda_display["cuda_versions"].apply(
                    lambda x: ", ".join(x) if isinstance(x, list) and x else "None"
                )
                cuda_display["cuda_count"] = cuda_display["cuda_versions"].apply(
                    lambda x: len(x) if isinstance(x, list) else 0
                )
                
                st.dataframe(
                    cuda_display[["ip", "owner", "cuda_count", "cuda_versions_str"]],
                    column_config={
                        "ip": st.column_config.TextColumn("IP Address"),
                        "owner": st.column_config.TextColumn("담당자"),
                        "cuda_count": st.column_config.NumberColumn("설치된 버전 수", format="%d"),
                        "cuda_versions_str": st.column_config.TextColumn("CUDA Versions"),
                    },
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("CUDA 정보가 없습니다. Refresh Data를 눌러 데이터를 수집하세요.")

    

except Exception as e:
    st.error(f"Error loading or displaying data: {e}")
