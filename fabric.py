
"""
Fabric Automation MCP Server (FastMCP) with robust .env loading + diagnostics.

.env keys required:
  TENANT_ID=
  CLIENT_ID=
  CLIENT_SECRET=
  USER_OBJECT_ID=
  CAPACITY_ID=
"""

import os
import json
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv, find_dotenv
from fastmcp import FastMCP

# --------------------
# Env loading (robust)
# --------------------
dotenv_path = find_dotenv(usecwd=True)
loaded = load_dotenv(dotenv_path=dotenv_path, override=True)
# Optionally log where we loaded from:
ENV_SOURCE = dotenv_path if loaded else "(no .env found)"

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_OBJECT_ID = os.getenv("USER_OBJECT_ID")
CAPACITY_ID = os.getenv("CAPACITY_ID")

REQUIRED = {
    "TENANT_ID": TENANT_ID,
    "CLIENT_ID": CLIENT_ID,
    "CLIENT_SECRET": CLIENT_SECRET,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    raise RuntimeError(
        f"Missing required .env values: {', '.join(missing)}. "
        f"WorkingDir={os.getcwd()} EnvSource={ENV_SOURCE}"
    )

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

# --------------------
# Auth helper
# --------------------
def get_access_token() -> str:
    """
    Acquire a token using client credentials. Raises on failure.
    """
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://api.fabric.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, data=payload)
    # If 401 or 400, raise to surface the exact cause
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        # Add more details to help diagnose
        raise RuntimeError(
            f"Token request failed: {e}\n"
            f"Status={resp.status_code}\n"
            f"Body={resp.text}\n"
            f"Tenant={TENANT_ID} ClientId={CLIENT_ID[:6]}... "
            f"EnvSource={ENV_SOURCE}"
        )
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {json.dumps(data, indent=2)}")
    return token

def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# --------------------
# MCP server
# --------------------
mcp = FastMCP("fabric-mcp")

# --------------------
# Diagnostics tools
# --------------------
@mcp.tool()
def mcp_env_info() -> Dict[str, Any]:
    """
    Returns diagnostic info about env loading for troubleshooting (secrets redacted).
    """
    return {
        "cwd": os.getcwd(),
        "env_source": ENV_SOURCE,
        "has_tenant_id": bool(TENANT_ID),
        "has_client_id": bool(CLIENT_ID),
        "has_client_secret": bool(CLIENT_SECRET),
        "has_user_object_id": bool(USER_OBJECT_ID),
        "has_capacity_id": bool(CAPACITY_ID),
    }

@mcp.tool()
def test_token() -> Dict[str, Any]:
    """
    Attempts to fetch a token and returns basic claims (redacted).
    """
    token = get_access_token()
    # Return short preview of token length to confirm issuance without dumping secrets
    return {"access_token_len": len(token)}

# --------------------
# Fabric tools
# --------------------
@mcp.tool()
def get_existing_workspaces() -> Dict[str, str]:
    """
    Returns {displayName: id} for existing workspaces.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 401:
        return {"error": "401 Unauthorized", "body": resp.text}
    resp.raise_for_status()
    data = resp.json()
    return {ws["displayName"]: ws["id"] for ws in data.get("value", [])}

@mcp.tool()
def create_workspace(name: str, capacity_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Creates a Fabric workspace. capacity_id defaults to CAPACITY_ID from .env.
    """
    cid = capacity_id or CAPACITY_ID
    if not cid:
        return {"error": "capacity_id not provided and CAPACITY_ID missing in .env"}

    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces"
    payload = {"displayName": name, "capacityId": cid}
    resp = requests.post(url, json=payload, headers=_headers(token))
    if resp.status_code == 201:
        return {"workspace_id": resp.json().get("id")}
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def assign_workspace_admin(workspace_id: str, user_object_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Assigns Admin role to the given user for a workspace.
    """
    uid = user_object_id or USER_OBJECT_ID
    if not uid:
        return {"error": "user_object_id not provided and USER_OBJECT_ID missing in .env"}

    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces/{workspace_id}/roleAssignments"
    payload = {"principal": {"id": uid, "type": "User"}, "role": "Admin"}
    resp = requests.post(url, json=payload, headers=_headers(token))
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def create_workspace_folder(workspace_id: str, folder_name: str, parent_folder_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Creates a folder in a workspace.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces/{workspace_id}/folders"
    payload = {"displayName": folder_name}
    if parent_folder_id:
        payload["parentFolderId"] = parent_folder_id
    resp = requests.post(url, json=payload, headers=_headers(token))
    if resp.status_code == 201:
        j = resp.json()
        return {"folder_id": j.get("id"), "displayName": j.get("displayName")}
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def create_lakehouse(workspace_id: str, lakehouse_name: str) -> Dict[str, Any]:
    """
    Creates a Lakehouse in the specified workspace.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces/{workspace_id}/lakehouses"
    payload = {"displayName": lakehouse_name}
    resp = requests.post(url, json=payload, headers=_headers(token))
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def create_warehouse(workspace_id: str, warehouse_name: str) -> Dict[str, Any]:
    """
    Creates a Warehouse in the specified workspace.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/workspaces/{workspace_id}/warehouses"
    payload = {"displayName": warehouse_name}
    resp = requests.post(url, json=payload, headers=_headers(token))
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def create_deployment_pipeline(name: str, description: str, stages: List[str]) -> Dict[str, Any]:
    """
    Creates a deployment pipeline with given stage names.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/deploymentPipelines"
    payload = {
        "displayName": name,
        "description": description,
        "stages": [
            {"displayName": s, "description": f"{s} stage", "isPublic": False}
            for s in stages
        ],
    }
    resp = requests.post(url, json=payload, headers=_headers(token))
    if resp.status_code == 201:
        p = resp.json()
        stage_map = {s["displayName"]: s["id"] for s in p.get("stages", [])}
        return {"pipeline_id": p.get("id"), "stages": stage_map}
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def assign_workspace_to_stage(pipeline_id: str, stage_id: str, workspace_id: str) -> Dict[str, Any]:
    """
    Assigns a workspace to a deployment pipeline stage.
    """
    token = get_access_token()
    url = f"{FABRIC_BASE}/deploymentPipelines/{pipeline_id}/stages/{stage_id}/assignWorkspace"
    payload = {"workspaceId": workspace_id}
    resp = requests.post(url, json=payload, headers=_headers(token))
    return {"status": resp.status_code, "body": resp.text}

@mcp.tool()
def assign_pipeline_admin(pipeline_id: str, user_object_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Assigns Admin role on a deployment pipeline to the given user.
    """
    uid = user_object_id or USER_OBJECT_ID
    if not uid:
        return {"error": "user_object_id not provided and USER_OBJECT_ID missing in .env"}
    token = get_access_token()
    url = f"{FABRIC_BASE}/deploymentPipelines/{pipeline_id}/roleAssignments"
    payload = {"principal": {"id": uid, "type": "User"}, "role": "Admin"}
    resp = requests.post(url, json=payload, headers=_headers(token))
    return {"status": resp.status_code, "body": resp.text}

# --------------------
# Runner
# --------------------
if __name__ == "__main__":
    API_KEY = os.getenv("MCP_API_KEY")
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=8000,
        headers={"Authorization": f"Bearer {API_KEY}"}
    )

