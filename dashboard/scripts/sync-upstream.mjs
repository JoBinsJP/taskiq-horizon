import { cp, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dashboardRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(dashboardRoot, "..", "..");

const patches = [
  {
    file: "src/js/base.js",
    from: `        jobBaseName(name) {\n            if (!name.includes('\\\\')) return name;\n\n            var parts = name.split('\\\\');\n\n            return parts[parts.length - 1];\n        },`,
    to: `        jobBaseName(name) {\n            if (!name) {\n                return name;\n            }\n\n            return name.split(/[\\\\\\\\:.]/).pop();\n        },`,
  },
  {
    file: "src/js/screens/recentJobs/job.vue",
    from: `    import phpunserialize from 'phpunserialize';\n    import moment from 'moment-timezone';\n    import StackTrace from './../../components/Stacktrace.vue';\n\n    export default {\n        components: {\n            'stack-trace': StackTrace,\n        },`,
    to: `    import moment from 'moment-timezone';\n    import { decodeSerializedCommand, prettyPrintSerializedJobData } from '@/support/job-data';\n\n    export default {`,
  },
  {
    file: "src/js/screens/recentJobs/job.vue",
    from: `            unserialized() {\n                return phpunserialize(this.job.payload.data.command);\n            },`,
    to: `            unserialized() {\n                return decodeSerializedCommand(this.job.payload?.data?.command);\n            },`,
  },
  {
    file: "src/js/screens/recentJobs/job.vue",
    from: `                }\n\n                return null;`,
    to: `                } else if (this.job.delay > 0) {\n                    return this.formatDate(this.job.payload.pushedAt).add(this.job.delay, 'seconds')\n                        .local()\n                        .format('YYYY-MM-DD HH:mm:ss');\n                }\n\n                return null;`,
  },
  {
    file: "src/js/screens/recentJobs/job.vue",
    from: `                    return data.command && !data.command.includes('CallQueuedClosure')\n                        ? phpunserialize(data.command) : data;\n                } catch (err) {\n                    return data;\n                }`,
    to: `                    return prettyPrintSerializedJobData(data);\n                } catch (err) {\n                    return data;\n                }`,
  },
  {
    file: "src/js/screens/recentJobs/job-row.vue",
    from: `    import phpunserialize from 'phpunserialize'\n    import moment from 'moment-timezone';`,
    to: `    import moment from 'moment-timezone';\n    import { decodeSerializedCommand } from '@/support/job-data';`,
  },
  {
    file: "src/js/screens/recentJobs/job-row.vue",
    from: `                    return phpunserialize(this.job.payload.data.command);`,
    to: `                    return decodeSerializedCommand(this.job.payload?.data?.command);`,
  },
  {
    file: "src/js/screens/monitoring/job-row.vue",
    from: `    import phpunserialize from 'phpunserialize'\n    import moment from 'moment-timezone';`,
    to: `    import moment from 'moment-timezone';\n    import { decodeSerializedCommand } from '@/support/job-data';`,
  },
  {
    file: "src/js/screens/monitoring/job-row.vue",
    from: `                    return phpunserialize(this.job.payload.data.command);`,
    to: `                    return decodeSerializedCommand(this.job.payload?.data?.command);`,
  },
  {
    file: "src/js/screens/monitoring/job-row.vue",
    from: `                if (this.unserialized && this.unserialized.delay) {\n                    return moment.tz(this.unserialized.delay.date, this.unserialized.delay.timezone)\n                        .fromNow(true);`,
    to: `                if (this.unserialized && this.unserialized.delay && this.unserialized.delay.date) {\n                    return moment.tz(this.unserialized.delay.date, this.unserialized.delay.timezone)\n                        .fromNow(true);\n                } else if (this.unserialized && this.unserialized.delay) {\n                    return this.formatDate(this.job.payload.pushedAt).add(this.unserialized.delay, 'seconds')\n                        .fromNow(true);`,
  },
  {
    file: "src/js/screens/failedJobs/job.vue",
    from: `    import phpunserialize from 'phpunserialize'\n    import StackTrace from '@/components/Stacktrace.vue'`,
    to: `    import StackTrace from '@/components/Stacktrace.vue'\n    import { prettyPrintSerializedJobData } from '@/support/job-data'`,
  },
  {
    file: "src/js/screens/failedJobs/job.vue",
    from: `                    return data.command && !data.command.includes('CallQueuedClosure')\n                        ? phpunserialize(data.command) : data;\n                } catch (err) {\n                    return data;\n                }`,
    to: `                    return prettyPrintSerializedJobData(data);\n                } catch (err) {\n                    return data;\n                }`,
  },
  {
    file: "src/js/components/LineChart.vue",
    from: `    import Chart from 'chart.js';`,
    to: `    import Chart from 'chart.js/auto';`,
  },
  {
    file: "src/js/components/LineChart.vue",
    from: `            this.chart = new Chart(this.context, {\n                type: 'line',\n                options: {\n                    tooltips: {\n                        intersect: false,\n                    },\n                    legend: {\n                        display: false,\n                    },\n                    scales: {\n                        yAxes: [\n                            {\n                                ticks: {\n                                    beginAtZero: true,\n                                     callback: (value, index, values) => {\n                                        return this.data.datasets[0].label === "Seconds"\n                                            ? \`\${value} secs\`\n                                            : value;\n                                    },\n                                },\n                                gridLines: {\n                                    display: true\n                                },\n                                beforeBuildTicks: function (scale) {\n                                    var max = scale.chart.data.datasets[0].data.reduce((max, value) => value > max ? value : max)\n\n                                    scale.max = parseFloat(max) + parseFloat(max * 0.25);\n                                },\n                            }\n                        ],\n                        xAxes: [\n                            {\n                                gridLines: {\n                                    display: true\n                                },\n                                afterTickToLabelConversion: function (data) {\n                                    var xLabels = data.ticks;\n\n                                    xLabels.forEach(function (labels, i) {\n                                        if (i % 6 != 0 && (i + 1) != xLabels.length) {\n                                            xLabels[i] = '';\n                                        }\n                                    });\n                                }\n                            },\n                        ]\n                    }\n                },\n                data: this.data\n            });\n        },`,
    to: `            this.chart = new Chart(this.context, {\n                type: 'line',\n                data: this.data,\n                options: {\n                    interaction: {\n                        intersect: false,\n                    },\n                    plugins: {\n                        legend: {\n                            display: false,\n                        },\n                    },\n                    scales: {\n                        y: {\n                            beginAtZero: true,\n                            ticks: {\n                                callback: (value) => {\n                                    return this.data.datasets[0].label === "Seconds"\n                                        ? \`\${value} secs\`\n                                        : value;\n                                },\n                            },\n                            grid: {\n                                display: true,\n                            },\n                            afterDataLimits(scale) {\n                                const max = scale.chart.data.datasets[0].data.reduce(\n                                    (currentMax, value) => value > currentMax ? value : currentMax,\n                                    0,\n                                );\n\n                                scale.max = parseFloat(max) + parseFloat(max * 0.25);\n                            },\n                        },\n                        x: {\n                            grid: {\n                                display: true,\n                            },\n                            ticks: {\n                                callback(value, index) {\n                                    const labels = this.getLabels();\n\n                                    if (index % 6 !== 0 && index + 1 !== labels.length) {\n                                        return "";\n                                    }\n\n                                    return labels[index];\n                                },\n                            },\n                        },\n                    }\n                },\n            });\n        },\n\n        beforeUnmount() {\n            this.chart?.destroy();\n        },`,
  },
];

async function syncDirectory(source, target) {
  await mkdir(target, { recursive: true });
  await cp(source, target, { recursive: true, force: true });
}

async function applyPatches() {
  for (const patch of patches) {
    const filePath = path.join(dashboardRoot, patch.file);
    let source = await readFile(filePath, "utf8");
    if (!source.includes(patch.to)) {
      if (!source.includes(patch.from)) {
        throw new Error(`Patch source not found in ${patch.file}`);
      }
      source = source.replace(patch.from, patch.to);
      await writeFile(filePath, source);
    }
  }

  const supportPath = path.join(dashboardRoot, "src/js/support/job-data.js");
  await mkdir(path.dirname(supportPath), { recursive: true });
  await writeFile(
    supportPath,
    `export function decodeSerializedCommand(command) {\n    if (typeof command !== "string") {\n        return null;\n    }\n\n    const normalized = command.trim();\n\n    if (!normalized || normalized.includes("CallQueuedClosure")) {\n        return null;\n    }\n\n    if (normalized.startsWith("{") || normalized.startsWith("[")) {\n        try {\n            return JSON.parse(normalized);\n        } catch (error) {\n            return null;\n        }\n    }\n\n    return null;\n}\n\nexport function prettyPrintSerializedJobData(data) {\n    const decoded = decodeSerializedCommand(data?.command);\n\n    return decoded ?? data;\n}\n`,
  );
}

await syncDirectory(path.join(repoRoot, "resources/js"), path.join(dashboardRoot, "src/js"));
await syncDirectory(path.join(repoRoot, "resources/sass"), path.join(dashboardRoot, "src/sass"));
await applyPatches();

console.log("Synced Laravel Horizon frontend source into taskiq-horizon/dashboard/src");
