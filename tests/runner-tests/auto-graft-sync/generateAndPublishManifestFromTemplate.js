
const fs = require('fs');
const { execSync } = require('child_process');

// This takes a subgraphName, outPath, and Qm.. hash as a CLI input, which is the graft base.
const outPath = process.argv[2];
const graftBase = process.argv[3];
const graftBlock = process.argv[4];

const yamlPath = './template.yaml';
let yamlContent = fs.readFileSync(yamlPath, 'utf-8');
yamlContent = yamlContent.replace(/base: .+/, `base: ${graftBase}`);
yamlContent = yamlContent.replace(/block: .+/, `block: ${graftBlock}`);
fs.writeFileSync(outPath, yamlContent);
console.log("fuzzba")

// Assuming you have your IPFS_URI exported as environment variables.
// Instead of deploy, run graph build to -only upload to ipfs-.
execSync('graph build ' + outPath + ' --ipfs $IPFS_URI', {
    stdio: 'inherit'
});
