import { Route, Routes } from "react-router-dom";
import LobbyPage from "./pages/LobbyPage";
import LeaderboardPage from "./pages/LeaderboardPage";
import PlayerPage from "./pages/PlayerPage";
import LinkPage from "./pages/LinkPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<LobbyPage />} />
      <Route path="/leaderboard" element={<LeaderboardPage />} />
      <Route path="/leaderboard/:game" element={<LeaderboardPage />} />
      <Route path="/player/:playerId" element={<PlayerPage />} />
      <Route path="/link" element={<LinkPage />} />
      <Route path="/link/native-complete" element={<LinkPage nativeComplete />} />
      <Route path="*" element={<LobbyPage />} />
    </Routes>
  );
}
