// Rock Paper Scissors match logic — port of the demo's rps_begin / rps_round
// / game_over cases (web/index.html:786-855). The cumulative score resets
// ONLY on round 0. The round timer is clear-only in the demo (the server
// resolves the round after timeout_ms) — kept for parity.
import { notify, send, state } from "../lobby/store";

/** Enable/disable the choice buttons. */
export function setRpsButtons(enabled: boolean) {
  state.rpsButtonsEnabled = enabled;
  notify();
}

/** A choice button was clicked (0 = rock, 1 = paper, 2 = scissors). */
export function chooseRps(choice: number) {
  if (!state.matchToken || state.rpsChosen || !state.rpsButtonsEnabled) return;
  state.rpsChosen = true;
  setRpsButtons(false);
  state.rpsStatus = "You threw — waiting for the opponent…";
  notify();
  clearInterval(state.rpsTimer);
  state.rpsTimer = undefined;
  send({ type: "rps_choice", match_token: state.matchToken, choice });
}

/** rps_begin: a round is open — switch to the RPS panel and enable choices. */
export function beginRpsRound(msg: { round: number; player_a: string }) {
  state.gameMode = "rps";
  state.iAmPlayerA = state.playerId === msg.player_a;
  state.rpsRound = msg.round;
  state.rpsChosen = false;
  setRpsButtons(true);
  state.rpsStatus = "Round " + (msg.round + 1) + " — choose!";
  // Keep the cumulative score across rounds; only round 0 starts at 0-0.
  if (msg.round === 0) state.rpsScore = "You 0 – 0 Opponent";
  notify();
}

/** rps_round: the verdict for the completed round. */
export function rpsRoundResult(msg: {
  round: number;
  a_choice: number;
  b_choice: number;
  winner: string | null;
  a_score: number;
  b_score: number;
}) {
  clearInterval(state.rpsTimer);
  state.rpsTimer = undefined;
  const mine = state.iAmPlayerA ? msg.a_choice : msg.b_choice;
  const theirs = state.iAmPlayerA ? msg.b_choice : msg.a_choice;
  const myScore = state.iAmPlayerA ? msg.a_score : msg.b_score;
  const oppScore = state.iAmPlayerA ? msg.b_score : msg.a_score;
  state.rpsScore = "You " + myScore + " – " + oppScore + " Opponent";
  setRpsButtons(false);
  if (mine === 255 || theirs === 255) {
    const who = mine === 255 ? "You" : "The opponent";
    state.rpsStatus =
      who + " didn't choose" +
      (msg.winner === state.playerId ? " — you win the round!" : " — round lost.");
  } else {
    const face: [string, string][] = [
      ["✊", "rock"],
      ["✋", "paper"],
      ["✌", "scissors"],
    ];
    const myTxt = face[mine]?.[0] + " " + face[mine]?.[1];
    const oppTxt = face[theirs]?.[0] + " " + face[theirs]?.[1];
    const verdict =
      msg.winner === null
        ? "Draw!"
        : msg.winner === state.playerId
          ? "You win the round!"
          : "Opponent wins the round.";
    state.rpsStatus = "You: " + myTxt + " · Opp: " + oppTxt + " — " + verdict;
  }
  notify();
}

/** game_over while in RPS mode — the match verdict. */
export function rpsGameOver(winner: string) {
  clearInterval(state.rpsTimer);
  state.rpsTimer = undefined;
  setRpsButtons(false);
  if (state.gameMode === "rps") {
    state.rpsStatus = winner === state.playerId ? "You win the match!" : "You lose the match.";
    notify();
  }
}
