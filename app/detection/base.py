from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np

from app.common.schemas import Detection


class Detector(ABC):
    @abstractmethod
    def detect(self, frame: np.ndarray) -> list[Detection]:
        raise NotImplementedError
