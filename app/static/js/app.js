// Общие утилиты
function formatDuration(ms) {
    if (ms < 1000) return `${ms}мс`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}с`;
    return `${(ms / 60000).toFixed(2)}м`;
}

function formatDate(dateString) {
    return new Date(dateString).toLocaleString('ru-RU');
}