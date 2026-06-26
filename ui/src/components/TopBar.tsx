import { getToken, setToken } from "../api.ts";

export function TopBar(): React.ReactElement {
  const token = getToken();
  return (
    <header className="topbar">
      <span className="topbar__brand">Vibemon</span>
      <span className="faint" style={{ fontSize: "var(--fs-sm)" }}>microVM console</span>
      <span className="topbar__spacer" />
      <input
        className="input topbar__token"
        type="password"
        placeholder="API token"
        defaultValue={token}
        spellCheck={false}
        onChange={(e) => setToken(e.currentTarget.value)}
        aria-label="API bearer token"
      />
    </header>
  );
}
