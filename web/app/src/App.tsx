import { Route, Routes } from "react-router-dom";
import LobbyPage from "./pages/LobbyPage";
import LeaderboardPage from "./pages/LeaderboardPage";
import PlayerPage from "./pages/PlayerPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<LobbyPage />} />
      <Route path="/leaderboard/:game" element={<LeaderboardPage />} />
      <Route path="/player/:playerId" element={<PlayerPage />} />
      <Route path="*" element={<LobbyPage />} />
    </Routes>
  );
}
