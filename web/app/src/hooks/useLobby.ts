// React subscription to the lobby store: re-render on every notify() (the
// store is mutable; discrete protocol events notify, the 30Hz frame path
// does not — components read state fields during render).
import { useEffect, useReducer } from "react";
import { state, subscribe } from "../lobby/store";

export function useLobby() {
  const [, force] = useReducer((x: number) => x + 1, 0);
  useEffect(() => subscribe(force), []);
  return state;
}
