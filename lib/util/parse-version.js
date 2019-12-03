module.exports = function parseVersion(versionString) {
  if (!versionString) {
    return { major: 0, minor: 0, patch: 0 };
  }
  const v = String(versionString).split('.');
  return {
    major: parseInt(v[0], 10) || 0,
    minor: parseInt(v[1], 10) || 0,
    patch: parseInt(v[2], 10) || 0
  };
};
