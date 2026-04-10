from __future__ import annotations


class SchemaError(Exception):
    def __init__(self, code: str, message: str, hint: str = "", file_path: str | None = None) -> None:
        self.code = code
        self.message = message
        self.hint = hint
        self.file_path = file_path
        super().__init__(self.__str__())

    def __str__(self) -> str:
        parts = [f"[{self.code}] {self.message}"]
        if self.file_path:
            parts.append(f"file={self.file_path}")
        if self.hint:
            parts.append(f"hint={self.hint}")
        return " | ".join(parts)
