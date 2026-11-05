"""Pinned COL-02 NumPy CPU-dispatch policy; no model/math/package changes.

Configure only this Python process before importing NumPy. Actual disabled
features are checked before fitting, so setting a late env flag cannot pass.
"""
import os
import platform

POLICY_ID = "col02-numpy222-portable-cpu-v1"
X86_FEATURES = ("AVX2", "AVX512F", "AVX512_SKX", "AVX512_CLX", "AVX512_CNL", "AVX512_ICL")
ENV_VALUE = ",".join(X86_FEATURES)
X86 = {"x86_64", "amd64"}
ARM = {"arm64", "aarch64"}


def require(ok, reason):
    if not ok: raise ValueError(reason)


def configure_process():
    architecture = platform.machine().lower()
    require(architecture in X86 | ARM, "COL02_UNSUPPORTED_CPU_ARCHITECTURE")
    expected = ENV_VALUE if architecture in X86 else ""
    existing = os.environ.get("NPY_DISABLE_CPU_FEATURES", "")
    require(not existing or existing == expected, "COL02_FOREIGN_NUMPY_CPU_POLICY")
    if expected: os.environ["NPY_DISABLE_CPU_FEATURES"] = expected


def validate_evidence(proof):
    architecture = proof["architecture"]
    require(proof["policy_id"] == POLICY_ID and proof["numpy_version"] == "2.2.2"
            and proof["x86_disable_features"] == list(X86_FEATURES)
            and architecture in X86 | ARM, "COL02_NUMERICAL_RUNTIME_POLICY_CHANGED")
    flags = proof["actual_cpu_features"]
    if architecture in X86:
        require(proof["environment_value"] == ENV_VALUE and set(flags) == set(X86_FEATURES)
                and all(value is False for value in flags.values()), "COL02_NUMPY_DISPATCH_STILL_ACTIVE")
    else:
        require(proof["environment_value"] == "" and flags == {}, "COL02_ARM_CPU_POLICY_CHANGED")


def evidence():
    import numpy as np
    architecture = platform.machine().lower()
    flags = {}
    if architecture in X86:
        # This inspection is version-bound to the exact NumPy 2.2.2 lock.
        from numpy._core._multiarray_umath import __cpu_features__
        flags = {name: bool(__cpu_features__[name]) for name in X86_FEATURES}
    proof = {"policy_id": POLICY_ID, "numpy_version": np.__version__, "architecture": architecture,
             "x86_disable_features": list(X86_FEATURES),
             "environment_value": os.environ.get("NPY_DISABLE_CPU_FEATURES", ""), "actual_cpu_features": flags}
    validate_evidence(proof)
    return proof
