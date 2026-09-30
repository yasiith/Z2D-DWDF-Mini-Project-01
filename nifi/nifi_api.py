"""
Minimal client for the Apache NiFi 2.x REST API (standard library only).

Used by build_flow.py to create the ABC Hub ETL flow in a running NiFi
instance and to export it as a flow definition. Only the calls the builder
needs are implemented.
"""
import json
import time
import urllib.error
import urllib.request


class NiFiError(RuntimeError):
    pass


class NiFi:
    def __init__(self, base_url="http://127.0.0.1:8080/nifi-api"):
        self.base = base_url.rstrip("/")
        self._types = None

    # ------------------------------------------------------------------ http
    def _call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raise NiFiError(f"{method} {path} -> {e.code}: {e.read().decode(errors='replace')}") from None

    def get(self, path):
        return self._call("GET", path)

    def post(self, path, body):
        return self._call("POST", path, body)

    def put(self, path, body):
        return self._call("PUT", path, body)

    def delete(self, path):
        return self._call("DELETE", path)

    # --------------------------------------------------------------- lookups
    def root_id(self):
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]

    def bundle(self, short_type):
        """Resolve a processor / controller-service short name to (type, bundle)."""
        if self._types is None:
            self._types = {}
            for key in ("processorTypes", "controllerServiceTypes"):
                path = "/flow/processor-types" if key == "processorTypes" else "/flow/controller-service-types"
                for t in self.get(path)[key]:
                    self._types[t["type"].split(".")[-1]] = (t["type"], t["bundle"])
        if short_type not in self._types:
            raise NiFiError(f"Unknown component type {short_type}")
        return self._types[short_type]

    @staticmethod
    def _rev(entity=None):
        return {"version": entity["revision"]["version"] if entity else 0}

    # ------------------------------------------------------------ components
    def create_parameter_context(self, name, description, parameters):
        body = {"revision": {"version": 0},
                "component": {"name": name, "description": description,
                              "parameters": [{"parameter": p} for p in parameters]}}
        return self.post("/parameter-contexts", body)

    def create_process_group(self, parent_id, name, x, y, comments="", parameter_context_id=None):
        comp = {"name": name, "position": {"x": x, "y": y}, "comments": comments}
        if parameter_context_id:
            comp["parameterContext"] = {"id": parameter_context_id}
        return self.post(f"/process-groups/{parent_id}/process-groups",
                         {"revision": {"version": 0}, "component": comp})

    def create_controller_service(self, pg_id, short_type, name, properties, comments=""):
        ctype, bundle = self.bundle(short_type)
        body = {"revision": {"version": 0},
                "component": {"type": ctype, "bundle": bundle, "name": name,
                              "comments": comments, "properties": properties}}
        return self.post(f"/process-groups/{pg_id}/controller-services", body)

    def enable_controller_service(self, cs_id):
        cs = self.get(f"/controller-services/{cs_id}")
        self.put(f"/controller-services/{cs_id}/run-status",
                 {"revision": self._rev(cs), "state": "ENABLED"})

    def disable_controller_service(self, cs_id):
        cs = self.get(f"/controller-services/{cs_id}")
        self.put(f"/controller-services/{cs_id}/run-status",
                 {"revision": self._rev(cs), "state": "DISABLED"})

    def create_processor(self, pg_id, short_type, name, x, y, properties=None, schedule=None,
                         auto_terminate=(), comments="", concurrency=1, penalty="30 sec",
                         yield_duration="1 sec", bulletin="WARN", retry=None):
        ptype, bundle = self.bundle(short_type)
        schedule = schedule or {}
        config = {
            "properties": properties or {},
            "schedulingStrategy": schedule.get("strategy", "TIMER_DRIVEN"),
            "schedulingPeriod": schedule.get("period", "0 sec"),
            "autoTerminatedRelationships": list(auto_terminate),
            "comments": comments,
            "concurrentlySchedulableTaskCount": concurrency,
            "penaltyDuration": penalty,
            "yieldDuration": yield_duration,
            "bulletinLevel": bulletin,
        }
        if retry:
            config.update({"retriedRelationships": retry["relationships"],
                           "retryCount": retry.get("count", 3),
                           "backoffMechanism": "PENALIZE_FLOWFILE",
                           "maxBackoffPeriod": retry.get("max_backoff", "5 mins")})
        body = {"revision": {"version": 0},
                "component": {"type": ptype, "bundle": bundle, "name": name,
                              "position": {"x": x, "y": y}, "config": config}}
        return self.post(f"/process-groups/{pg_id}/processors", body)

    def create_port(self, pg_id, kind, name, x, y, comments=""):
        path = "input-ports" if kind == "INPUT_PORT" else "output-ports"
        return self.post(f"/process-groups/{pg_id}/{path}",
                         {"revision": {"version": 0},
                          "component": {"name": name, "position": {"x": x, "y": y}, "comments": comments}})

    def create_funnel(self, pg_id, x, y):
        """A funnel merges several connections into one (keeps the canvas readable)."""
        return self.post(f"/process-groups/{pg_id}/funnels",
                         {"revision": {"version": 0}, "component": {"position": {"x": x, "y": y}}})

    def create_label(self, pg_id, text, x, y, width=400, height=60, font_size="14px", color="#fff7d7"):
        return self.post(f"/process-groups/{pg_id}/labels",
                         {"revision": {"version": 0},
                          "component": {"label": text, "position": {"x": x, "y": y},
                                        "width": width, "height": height,
                                        "style": {"font-size": font_size, "background-color": color}}})

    def connect(self, pg_id, source, dest, relationships=(), name="", bends=None,
                back_pressure_count=10000, back_pressure_size="1 GB", expiration="0 sec"):
        """source/dest: dicts with id, groupId, type (PROCESSOR / INPUT_PORT / OUTPUT_PORT)."""
        comp = {"name": name,
                "source": source, "destination": dest,
                "selectedRelationships": list(relationships),
                "backPressureObjectThreshold": back_pressure_count,
                "backPressureDataSizeThreshold": back_pressure_size,
                "flowFileExpiration": expiration}
        if bends:
            comp["bends"] = [{"x": x, "y": y} for x, y in bends]
        return self.post(f"/process-groups/{pg_id}/connections",
                         {"revision": {"version": 0}, "component": comp})

    # ------------------------------------------------------------- run state
    def set_processor_state(self, proc_id, state):
        """state: RUNNING | STOPPED | RUN_ONCE | DISABLED"""
        p = self.get(f"/processors/{proc_id}")
        return self.put(f"/processors/{proc_id}/run-status", {"revision": self._rev(p), "state": state})

    def set_group_state(self, pg_id, state):
        return self.put(f"/flow/process-groups/{pg_id}", {"id": pg_id, "state": state})

    def enable_group_services(self, pg_id):
        return self.put(f"/flow/process-groups/{pg_id}/controller-services",
                        {"id": pg_id, "state": "ENABLED"})

    def processor_validation_errors(self, pg_id):
        """Returns {processor name: [errors]} for every invalid processor below pg_id."""
        out = {}
        for p in self.get(f"/process-groups/{pg_id}/processors?includeDescendantGroups=true")["processors"]:
            errs = p["component"].get("validationErrors") or []
            if errs:
                out[p["component"]["name"]] = errs
        return out

    def clear_processor_state(self, proc_id):
        return self.post(f"/processors/{proc_id}/state/clear-requests", {})

    def wait_for_queues_empty(self, pg_id, timeout=600, settle=5):
        """Blocks until no FlowFiles are queued in pg_id (or timeout)."""
        deadline = time.time() + timeout
        quiet_since = None
        while time.time() < deadline:
            status = self.get(f"/flow/process-groups/{pg_id}/status?recursive=true")
            snap = status["processGroupStatus"]["aggregateSnapshot"]
            busy = snap["flowFilesQueued"] > 0 or snap.get("activeThreadCount", 0) > 0
            if not busy:
                quiet_since = quiet_since or time.time()
                if time.time() - quiet_since >= settle:
                    return True
            else:
                quiet_since = None
            time.sleep(1)
        return False

    def download_flow(self, pg_id):
        return self.get(f"/process-groups/{pg_id}/download?includeReferencedServices=true")
