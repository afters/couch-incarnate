
var Queue = function () {

  var array = [];

  this.enqueue = function (element) {
    array.unshift(element);
  }

  this.dequeue = function () {
    return array.pop();
  }

  this.peek = function (queueIndex) {
    queueIndex = queueIndex || 0;
    return array[array.length - 1 - queueIndex];
  }

  this.getLength = function () {
    return array.length;
  }
}

module.exports = Queue;
