ssh dsan3
sudo su -
find /net/mss1/archive/mtn/201411?? -name "*.hdr" -print0 | xargs -0 egrep -f /home/$ME/cadence/patterns.dat > /home/$ME/cadence/cadence-201411.out
