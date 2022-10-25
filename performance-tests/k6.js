import { check } from "k6";
import http from "k6/http";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";
import { githubComment } from "https://raw.githubusercontent.com/dotansimha/k6-github-pr-comment/master/lib.js";

const DURATION = 30;
const VUS = 10;

function buildOptions(scenarioToThresholdsMap) {
  const result = {
    scenarios: {},
    thresholds: {},
  };

  let index = 0;

  for (const [scenario, thresholds] of Object.entries(
    scenarioToThresholdsMap
  )) {
    result.scenarios[scenario] = {
      executor: "constant-vus",
      exec: "run",
      startTime: DURATION * index + "s",
      vus: VUS,
      duration: DURATION + "s",
      env: { MODE: scenario },
      tags: { mode: scenario },
    };

    index++;
  }

  return result;
}

export const options = buildOptions({
  "simple-query": {
    no_errors: ["rate>=1.0"],
    expected_result: ["rate>=1.0"],
    http_req_duration: ["p(95)<=65", "avg<=50"],
  },
});

function checkNoErrors(resp) {
  try {
    return !("errors" in resp.json());
  } catch (error) {
    console.error(error);
    return false;
  }
}

export function handleSummary(data) {
  githubComment(data, {
    token: __ENV.GITHUB_TOKEN,
    commit: __ENV.GITHUB_SHA,
    pr: __ENV.GITHUB_PR,
    org: "graphprotocol",
    repo: "graph-node",
    renderTitle({ passes }) {
      return passes ? "✅ Benchmark Results" : "❌ Benchmark Failed";
    },
    renderMessage({ passes, checks, thresholds }) {
      const result = [];

      if (thresholds.failures) {
        result.push(
          `**Performance regression detected**: it seems like your Pull Request adds some extra latency to the GraphQL query requests.`
        );
      }

      if (checks.failures) {
        result.push(
          "**Failed assertions detected**: some GraphQL operations included in the loadtest are failing."
        );
      }

      if (!passes) {
        result.push(
          `> If the performance regression is expected, please increase the failing threshold.`
        );
      }

      return result.join("\n");
    },
  });

  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

function graphql({ query, operationName, variables }) {
  const graphqlEndpoint =
    __ENV.GRAPHQL_ENDPOINT ||
    `http://${__ENV.GRAPHQL_HOSTNAME || "localhost"}:${
      __ENV.GRAPHQL_PORT || "10001"
    }/subgraphs/name/test/k6`;

  return http.post(
    graphqlEndpoint,
    JSON.stringify({
      query,
      operationName,
      variables,
    }),
    {
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
}

export function run() {
  const res = graphql({
    query: /* GraphQL */ `
      query test {
        purposes {
          id
          purpose
        }
      }
    `,
    variables: {},
    operationName: "test",
  });

  check(res, {
    no_errors: checkNoErrors,
    expected_result: (resp) => {
      const data = resp.json().data;

      return data && data.purposes[0].id;
    },
  });
}
