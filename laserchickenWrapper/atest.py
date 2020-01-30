#!/usr/bin/env python3

import importsetup
lcp1 = importsetup.setlaserchickenpath()

def main():
    print(lcp1)
    print('---')
    lcp2=importsetup.setlaserchickenpath()
    print(lcp2)


if __name__=='__main__':
    main()    
