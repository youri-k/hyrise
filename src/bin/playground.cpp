#include <iostream>

#include "types.hpp"

#include "cpucounters.h"

using namespace opossum;  // NOLINT

int main() {
  PCM * m = PCM::getInstance();
  PCM::ErrorCode returnResult = m->program();
  if (returnResult != PCM::Success){
     std::cerr << "Intel's PCM couldn't start" << std::endl;
     std::cerr << "Error code: " << returnResult << std::endl;
     exit(1);
  }

  std::cout << "Number of Cores: " << m->getNumCores() << std::endl;
  std::cout << "Number of Sockets: " << m->getNumSockets() << std::endl;
  std::cout << "QPI Links per Socket: " << m->getQPILinksPerSocket() << std::endl;

  SystemCounterState before_sstate = getSystemCounterState();

  std::cout << "Hello world!!" << std::endl;

  SystemCounterState after_sstate = getSystemCounterState();

  std::cout << "QPI Utilization: " << getOutgoingQPILinkUtilization(0, 0, before_sstate, after_sstate) << std::endl;

  // std::cout << "Instructions per clock:" << getIPC(before_sstate,after_sstate) << std::endl;
  // std::cout << "Bytes read:" << getBytesReadFromMC(before_sstate,after_sstate) << std::endl;
  m->cleanup();
  return 0;
}
